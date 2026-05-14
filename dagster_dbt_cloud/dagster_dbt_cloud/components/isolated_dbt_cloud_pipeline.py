"""Component that fans a dbt Cloud selection out into one Dagster asset per dbt model.

Each generated AssetsDefinition is its own op with its own retry boundary, so the
Dagster UI can retry or multi-select individual models. Every model materialization
issues a separate `DbtCloudCliInvocation.run()` against the same dbt Cloud CI job —
CI jobs allow concurrent runs of the same job, which is the workaround for dbt
Cloud deploy-job concurrency limits.

Inherits from upstream `DbtCloudComponent` so the manifest is fetched once and
cached via the state-backed component framework. All instances pointing at the
same workspace dedupe to a single state key (`workspace.unique_id`).

Customer YAML:

    type: dagster_dbt_cloud.components.IsolatedDbtCloudPipeline
    attributes:
      workspace:
        account_id: "{{ env.DBT_CLOUD_ACCOUNT_ID }}"
        token: "{{ env.DBT_API_KEY }}"
        project_id: "{{ env.DBT_CLOUD_PROJECT_ID }}"
        environment_id: "{{ env.DBT_CLOUD_ENVIRONMENT_ID }}"
        access_url: "{{ env.DBT_BASE_URL }}"
      select: "+file_orders_refined"
      git_branch: "{{ env.DBT_CLOUD_GIT_BRANCH }}"
      partition_keys: ["001", "002", "003"]
      deps: ["hygiene_mock"]
"""

import asyncio
import importlib
import json
from collections.abc import Callable, Iterator, Mapping, Sequence
from functools import cached_property
from pathlib import Path
from typing import Any, cast

import dagster as dg
from dagster.components import ComponentLoadContext
from dagster_dbt.asset_utils import (
    DAGSTER_DBT_CLOUD_ACCOUNT_ID_METADATA_KEY,
    DAGSTER_DBT_CLOUD_ENVIRONMENT_ID_METADATA_KEY,
    DAGSTER_DBT_CLOUD_PROJECT_ID_METADATA_KEY,
    DAGSTER_DBT_EXCLUDE_METADATA_KEY,
    DAGSTER_DBT_SELECT_METADATA_KEY,
    DAGSTER_DBT_SELECTOR_METADATA_KEY,
    DAGSTER_DBT_UNIQUE_ID_METADATA_KEY,
    build_dbt_specs,
)
from dagster_dbt.cloud_v2.cli_invocation import DbtCloudCliInvocation
from dagster_dbt.cloud_v2.client import (
    DAGSTER_ADHOC_TRIGGER_CAUSE,
    DbtCloudWorkspaceClient,
)
from dagster_dbt.cloud_v2.component.dbt_cloud_component import DbtCloudComponent
from dagster_dbt.cloud_v2.resources import DbtCloudWorkspace
from dagster_dbt.cloud_v2.sensor_builder import build_dbt_cloud_polling_sensor
from dagster_dbt.cloud_v2.types import DbtCloudWorkspaceData
from dagster_dbt.dbt_manifest import validate_manifest
from dagster_shared.serdes import deserialize_value
from pydantic import Field

from dagster_dbt_cloud.resources.github import GitHubResource

DBT_CLOUD_RUN_TIMEOUT_SECONDS = 1800
SENSOR_DEFAULT_INTERVAL_SECONDS = 300


def _safe_name(s: str) -> str:
    return "".join(c if c.isalnum() else "_" for c in s)


class BranchAwareDbtCloudClient(DbtCloudWorkspaceClient):
    """dbt Cloud client that forwards `git_branch` on job-run triggers.

    Upstream `trigger_job_run` doesn't accept `git_branch`, but CI jobs need it to
    know which branch to check out. Subclassing avoids the global monkey-patch.
    """

    git_branch: str | None = None

    def trigger_job_run(
        self,
        job_id: int,
        steps_override: Sequence[str] | None = None,
    ) -> Mapping[str, Any]:
        data: dict[str, Any] = {"cause": DAGSTER_ADHOC_TRIGGER_CAUSE}
        if self.git_branch:
            data["git_branch"] = self.git_branch
        if steps_override:
            data["steps_override"] = list(steps_override)
        return self._make_request(
            method="post",
            endpoint=f"jobs/{job_id}/run",
            base_url=self.api_v2_url,
            data=data,
        ).json()["data"]


def _build_branch_aware_client(
    workspace: DbtCloudWorkspace, git_branch: str | None
) -> BranchAwareDbtCloudClient:
    return BranchAwareDbtCloudClient(
        account_id=workspace.credentials.account_id,
        token=workspace.credentials.token,
        access_url=workspace.credentials.access_url,
        request_max_retries=workspace.request_max_retries,
        request_retry_delay=workspace.request_retry_delay,
        request_timeout=workspace.request_timeout,
        git_branch=git_branch,
    )


def _resolve_preflight(
    dotted_path: str,
) -> Callable[[dg.AssetExecutionContext], Any]:
    """Resolve `module.path:func` or `module.path.func` to a callable.

    Lazy lookup: only invoked when an asset op runs, so a broken import won't
    block code-server load for unrelated pipelines.
    """
    if ":" in dotted_path:
        module_path, _, attr = dotted_path.rpartition(":")
    else:
        module_path, _, attr = dotted_path.rpartition(".")
    if not module_path or not attr:
        raise ValueError(
            f"Invalid preflight path '{dotted_path}'. "
            "Expected 'package.module.func' or 'package.module:func'."
        )
    module = importlib.import_module(module_path)
    func = getattr(module, attr, None)
    if not callable(func):
        raise ValueError(
            f"Preflight target '{dotted_path}' is not callable (got {type(func).__name__})."
        )
    return func


def _log_compiled_sql(
    invocation: DbtCloudCliInvocation,
    context: dg.AssetExecutionContext,
    model_name: str,
) -> None:
    """Pull compiled SQL for the selected model from the dbt Cloud run and log it.

    Best-effort: artifact endpoints are flaky for failed runs, and we never want
    SQL logging to mask the real run error. We filter to the requested model so
    the log doesn't include unrelated compiled artifacts when CI bundles run.
    """
    run_id = invocation.run_handler.run_id
    client = invocation.client
    try:
        paths = [
            p
            for p in client.list_run_artifacts(run_id)
            if p.startswith("compiled/") and p.endswith(f"/{model_name}.sql")
        ]
        if not paths:
            context.log.warning(
                f"No compiled SQL artifact for '{model_name}' in run {run_id}."
            )
            return
        for path in paths:
            response = client._make_request(  # noqa: SLF001 — text artifact, not JSON
                method="get",
                endpoint=f"runs/{run_id}/artifacts/{path}",
                base_url=client.api_v2_url,
                session_attr="_get_artifact_session",
            )
            context.log.info(f"Compiled SQL [{path}]:\n\n{response.text}")
    except Exception as exc:
        context.log.warning(
            f"Could not fetch compiled SQL for run {run_id} ({model_name}): {exc}"
        )


def _fetch_single_ci_job_id(workspace: DbtCloudWorkspace) -> int:
    """Return the lone CI job id for the workspace's project+environment.

    Raises if zero or multiple CI jobs exist — be explicit instead of silently
    picking one. Customer can pin `ci_job_id` in YAML to skip this lookup.
    """
    jobs = workspace.get_client().list_jobs(
        project_id=int(workspace.project_id),
        environment_id=int(workspace.environment_id),
    )
    ci_jobs = [job for job in jobs if job.get("job_type") == "ci"]
    if not ci_jobs:
        raise ValueError(
            f"No dbt Cloud CI job found in project {workspace.project_id} / "
            f"environment {workspace.environment_id}."
        )
    if len(ci_jobs) > 1:
        names = [j.get("name") for j in ci_jobs]
        raise ValueError(
            f"Expected exactly one CI job in project {workspace.project_id} / "
            f"environment {workspace.environment_id}; found {len(ci_jobs)}: "
            f"{names}. Pin `ci_job_id` in the component YAML to disambiguate."
        )
    return int(ci_jobs[0]["id"])


class IsolatedDbtCloudPipeline(DbtCloudComponent):
    """Fan a dbt selection out into one AssetsDefinition per matched dbt model.

    Each model materializes via a single dbt Cloud CI-job run, so models within
    the same selection can execute concurrently and Dagster can retry / multi-
    select them individually.
    """

    git_branch: str | None = Field(
        default=None,
        description=(
            "Branch passed to dbt Cloud on every CI-job trigger. Required for CI "
            "jobs since their branch is set per-run, not on the job definition."
        ),
    )
    ci_job_id: int | None = Field(
        default=None,
        description=(
            "Optional explicit dbt Cloud CI job id. If unset, the component "
            "looks for exactly one CI job in the workspace's project+environment."
        ),
    )
    partition_keys: list[str] | None = Field(
        default=None,
        description=(
            "Static partition keys passed to dbt as --vars partition_id=KEY. "
            "Each generated asset is partitioned by this set."
        ),
    )
    deps: list[str] = Field(
        default_factory=list,
        description=(
            "Asset keys upstream of every model in this stage. Use to wire a "
            "dbt stage to a Python step in a sibling defs file."
        ),
    )
    preflight: str | None = Field(
        default=None,
        description=(
            "Dotted path to a callable that runs before every model in this "
            "stage materializes. Receives the AssetExecutionContext; raise any "
            "exception to abort the run before dbt Cloud is hit. Accepts "
            "'package.module.func' or 'package.module:func'."
        ),
    )
    github_repo: str | None = Field(
        default=None,
        description=(
            "GitHub repo in 'owner/name' form. When set, the component emits a "
            "sensor that polls this repo's `git_branch` HEAD and triggers a "
            "parse-job refresh on every new commit. Requires a `github` "
            "resource and `git_branch` on the component."
        ),
    )
    sensor_interval_seconds: int = Field(
        default=SENSOR_DEFAULT_INTERVAL_SECONDS,
        description="How often the git-watch sensor checks for new commits.",
    )

    @cached_property
    def resolved_ci_job_id(self) -> int:
        if self.ci_job_id is not None:
            return self.ci_job_id
        return _fetch_single_ci_job_id(self.workspace)

    @cached_property
    def partitions_def(self) -> dg.PartitionsDefinition | None:
        if not self.partition_keys:
            return None
        return dg.StaticPartitionsDefinition(self.partition_keys)

    def build_defs_from_state(
        self, context: ComponentLoadContext, state_path: Path | None
    ) -> dg.Definitions:
        # Emit the refresh job/sensor unconditionally so a fresh checkout
        # (state_path=None) still exposes a way to produce the first state.
        refresh_defs = self._build_refresh_defs(context)
        if state_path is None:
            return refresh_defs

        workspace_data = cast(
            "DbtCloudWorkspaceData", deserialize_value(state_path.read_text())
        )
        manifest = validate_manifest(workspace_data.manifest)
        nodes = manifest.get("nodes", {})

        asset_specs, check_specs = build_dbt_specs(
            manifest=manifest,
            translator=self.translator,
            select=self.select,
            exclude=self.exclude,
            selector=self.selector,
            io_manager_key=None,
            project=None,
        )

        upstream_dep_keys = [dg.AssetKey.from_user_string(d) for d in self.deps]

        assets: list[dg.AssetsDefinition] = []
        for spec in asset_specs:
            unique_id = spec.metadata.get(DAGSTER_DBT_UNIQUE_ID_METADATA_KEY)
            if unique_id is None or unique_id not in nodes:
                continue
            node = nodes[unique_id]
            if node.get("resource_type") != "model":
                continue
            assets.append(
                self._build_per_model_asset(
                    spec=spec,
                    check_specs=[cs for cs in check_specs if cs.asset_key == spec.key],
                    node=node,
                    extra_deps=upstream_dep_keys,
                    manifest=manifest,
                )
            )

        sensors = []
        if self.create_sensor:
            sensors.append(
                build_dbt_cloud_polling_sensor(
                    workspace=self.workspace,
                    dagster_dbt_translator=self.translator,
                )
            )

        return dg.Definitions.merge(
            dg.Definitions(assets=assets, sensors=sensors), refresh_defs
        )

    def _build_refresh_defs(self, context: ComponentLoadContext) -> dg.Definitions:
        """Emit the parse-job refresh job and git-watch sensor.

        Convention: only the component with `github_repo` set owns the refresh
        lifecycle for its workspace. For multi-stage pipelines that share a
        workspace, set `github_repo` on a single primary stage; siblings emit
        assets only. Two components both setting `github_repo` against the same
        workspace will fail load with a duplicate-job error — that's intentional
        and surfaces the misconfiguration immediately.
        """
        if not self.github_repo:
            return dg.Definitions()

        state_key = self.defs_state_config.key
        key_slug = _safe_name(state_key)
        op_name = f"refresh_{key_slug}_op"
        job_name = f"refresh_{key_slug}"
        project_root = context.project_root
        component_ref = self

        @dg.op(name=op_name)
        def _refresh_op() -> None:
            dg.get_dagster_logger().info(
                f"Refreshing dbt Cloud workspace state for '{state_key}'"
            )
            asyncio.run(component_ref.refresh_state(project_root))

        @dg.job(name=job_name)
        def _refresh_job() -> None:
            _refresh_op()

        sensor = self._build_change_sensor(
            refresh_job=_refresh_job,
            key_slug=key_slug,
        )

        return dg.Definitions(jobs=[_refresh_job], sensors=[sensor])

    def _build_change_sensor(
        self,
        *,
        refresh_job: dg.JobDefinition,
        key_slug: str,
    ) -> dg.SensorDefinition:
        """Polling sensor that triggers `refresh_job` on every new commit.

        Cursor stores the last-seen branch HEAD SHA. We update on trigger (not
        on job completion), so a failed parse won't be auto-retried — re-run
        the refresh job manually or push a new commit.
        """
        github_repo = self.github_repo
        git_branch = self.git_branch
        sensor_name = f"dbt_project_change_sensor_{key_slug}"

        @dg.sensor(
            name=sensor_name,
            job=refresh_job,
            minimum_interval_seconds=self.sensor_interval_seconds,
            default_status=dg.DefaultSensorStatus.RUNNING,
        )
        def _change_sensor(
            context: dg.SensorEvaluationContext, github: GitHubResource
        ) -> dg.SensorResult | dg.SkipReason:
            if not git_branch:
                return dg.SkipReason(
                    "Component has `github_repo` set but no `git_branch`; "
                    "sensor cannot determine which branch to poll."
                )
            current_sha = github.get_branch_head_sha(
                repo=cast("str", github_repo), branch=git_branch
            )
            if current_sha == context.cursor:
                return dg.SkipReason(
                    f"No new commit on {github_repo}@{git_branch} "
                    f"(HEAD={current_sha[:7]})"
                )
            context.update_cursor(current_sha)
            return dg.SensorResult(
                run_requests=[
                    dg.RunRequest(
                        run_key=current_sha,
                        tags={
                            "dbt_project_sha": current_sha,
                            "dbt_project_branch": git_branch,
                        },
                    )
                ],
            )

        return _change_sensor

    def _build_per_model_asset(
        self,
        *,
        spec: dg.AssetSpec,
        check_specs: Sequence[dg.AssetCheckSpec],
        node: Mapping[str, Any],
        extra_deps: Sequence[dg.AssetKey],
        manifest: Mapping[str, Any],
    ) -> dg.AssetsDefinition:
        workspace = self.workspace
        fqn = ".".join(node["fqn"])
        op_name = node["name"]
        ci_job_id = self.resolved_ci_job_id
        git_branch = self.git_branch
        translator = self.translator
        partitions_def = self.partitions_def
        preflight_path = self.preflight

        enriched_spec = spec.replace_attributes(
            kinds={"dbtcloud"} | (spec.kinds - {"dbt"}),
        ).merge_attributes(
            metadata={
                DAGSTER_DBT_CLOUD_ACCOUNT_ID_METADATA_KEY: workspace.credentials.account_id,
                DAGSTER_DBT_CLOUD_PROJECT_ID_METADATA_KEY: workspace.project_id,
                DAGSTER_DBT_CLOUD_ENVIRONMENT_ID_METADATA_KEY: workspace.environment_id,
            },
            deps=list(extra_deps) if extra_deps else [],
        )

        op_tags = {
            DAGSTER_DBT_SELECT_METADATA_KEY: fqn,
            DAGSTER_DBT_EXCLUDE_METADATA_KEY: "",
            DAGSTER_DBT_SELECTOR_METADATA_KEY: "",
        }

        @dg.multi_asset(
            name=op_name,
            specs=[enriched_spec],
            check_specs=list(check_specs),
            can_subset=True,
            op_tags=op_tags,
            partitions_def=partitions_def,
        )
        def _asset(context: dg.AssetExecutionContext) -> Iterator:
            if preflight_path:
                context.log.info(f"Preflight: {preflight_path}")
                _resolve_preflight(preflight_path)(context)

            args = ["build", "--select", fqn]
            if partitions_def is not None:
                args += ["--vars", json.dumps({"partition_id": context.partition_key})]

            context.log.info(
                f"dbt Cloud CI job {ci_job_id} :: {fqn} "
                f"(partition={context.partition_key if partitions_def else 'none'}, "
                f"branch={git_branch})"
            )

            client = _build_branch_aware_client(workspace, git_branch)
            invocation = DbtCloudCliInvocation.run(
                job_id=ci_job_id,
                args=args,
                client=client,
                manifest=manifest,
                dagster_dbt_translator=translator,
                context=context,
            )
            try:
                yield from invocation.wait(timeout=DBT_CLOUD_RUN_TIMEOUT_SECONDS)
            finally:
                _log_compiled_sql(invocation, context, model_name=op_name)

        return _asset
