"""Drop-in replacement for dagster_dbt.cloud_v2.dbt_cloud_assets, plus
build_dbt_chain_assets for per-model isolated dbt Cloud runs.

Fetches the dbt Cloud manifest once per workspace instance via
workspace.get_or_fetch_workspace_data() (cached on the workspace object), then
applies dbt node selection locally using build_dbt_specs — so N decorators
sharing the same workspace trigger exactly one parse job instead of N.

build_dbt_chain_assets returns one AssetsDefinition per dbt model matched by a
dbt node selection string (e.g. "+file_orders_refined", "tag:nightly"). Each
is its own op: independent retry boundaries, one DbtCloudCliInvocation.run()
per model-partition, and pool routing via hash((partition_key, unique_id)).
"""

import json
from collections.abc import Callable, Mapping, Sequence
from typing import Any

import dagster as dg
import requests
from dagster import (
    AssetsDefinition,
    BackfillPolicy,
    PartitionsDefinition,
    TimeWindowPartitionsDefinition,
    multi_asset,
)
from dagster._core.errors import DagsterInvariantViolationError
from dagster_dbt.asset_utils import (
    DAGSTER_DBT_CLOUD_ACCOUNT_ID_METADATA_KEY,
    DAGSTER_DBT_CLOUD_ENVIRONMENT_ID_METADATA_KEY,
    DAGSTER_DBT_CLOUD_PROJECT_ID_METADATA_KEY,
    DAGSTER_DBT_EXCLUDE_METADATA_KEY,
    DAGSTER_DBT_SELECT_METADATA_KEY,
    DAGSTER_DBT_SELECTOR_METADATA_KEY,
    DAGSTER_DBT_UNIQUE_ID_METADATA_KEY,
    DBT_DEFAULT_EXCLUDE,
    DBT_DEFAULT_SELECT,
    DBT_DEFAULT_SELECTOR,
    build_dbt_specs,
)
from dagster_dbt.cloud_v2.cli_invocation import DbtCloudCliInvocation
from dagster_dbt.cloud_v2.resources import DbtCloudWorkspace
from dagster_dbt.dagster_dbt_translator import DagsterDbtTranslator

DBT_CLOUD_RUN_TIMEOUT_SECONDS = 1800


def log_compiled_sql(
    invocation: "DbtCloudCliInvocation",
    context: dg.AssetExecutionContext,
) -> None:
    """Fetch compiled SQL from dbt Cloud run artifacts and write to Dagster logs."""
    run_id = invocation.run_handler.run_id
    client = invocation.client
    try:
        artifacts = client.list_run_artifacts(run_id)
        sql_paths = [
            a for a in artifacts if a.endswith(".sql") and a.startswith("compiled/")
        ]
        if not sql_paths:
            context.log.warning(f"No compiled SQL artifacts found for run {run_id}.")
            return
        headers = {
            "Authorization": f"Token {client.token}",
            "Content-Type": "application/json",
        }
        base = f"{client.access_url}/api/v2/accounts/{client.account_id}"
        for path in sql_paths:
            resp = requests.get(
                f"{base}/runs/{run_id}/artifacts/{path}",
                headers=headers,
                timeout=30,
            )
            resp.raise_for_status()
            context.log.info(f"Compiled SQL [{path}]:\n\n{resp.text}")
    except Exception as exc:
        context.log.warning(f"Could not fetch compiled SQL for run {run_id}: {exc}")


# 1 job to N models asset decorator
def dbt_cloud_assets(
    *,
    workspace: DbtCloudWorkspace,
    select: str = DBT_DEFAULT_SELECT,
    exclude: str = DBT_DEFAULT_EXCLUDE,
    selector: str = DBT_DEFAULT_SELECTOR,
    name: str | None = None,
    group_name: str | None = None,
    dagster_dbt_translator: DagsterDbtTranslator | None = None,
    partitions_def: PartitionsDefinition | None = None,
    backfill_policy: BackfillPolicy | None = None,
    deps: Any | None = None,
) -> Callable[[Callable[..., Any]], AssetsDefinition]:
    translator = dagster_dbt_translator or DagsterDbtTranslator()

    workspace_data = workspace.get_or_fetch_workspace_data()

    asset_specs, check_specs = build_dbt_specs(
        manifest=workspace_data.manifest,
        translator=translator,
        select=select,
        exclude=exclude,
        selector=selector,
        io_manager_key=None,
        project=None,
    )

    extra = {"deps": deps} if deps is not None else {}
    asset_specs = [
        spec.replace_attributes(
            kinds={"dbtcloud"} | spec.kinds - {"dbt"}
        ).merge_attributes(
            metadata={
                DAGSTER_DBT_CLOUD_ACCOUNT_ID_METADATA_KEY: workspace.credentials.account_id,
                DAGSTER_DBT_CLOUD_PROJECT_ID_METADATA_KEY: workspace_data.project_id,
                DAGSTER_DBT_CLOUD_ENVIRONMENT_ID_METADATA_KEY: workspace_data.environment_id,
            },
            **extra,
        )
        for spec in asset_specs
    ]

    if any(spec.group_name for spec in asset_specs) and group_name:
        raise DagsterInvariantViolationError(
            "Cannot set group_name on dbt_cloud_assets when one or more specs already have "
            "group_name set via the translator."
        )

    if (
        partitions_def
        and isinstance(partitions_def, TimeWindowPartitionsDefinition)
        and not backfill_policy
    ):
        backfill_policy = BackfillPolicy.single_run()

    op_tags = {
        DAGSTER_DBT_SELECT_METADATA_KEY: select,
        DAGSTER_DBT_EXCLUDE_METADATA_KEY: exclude,
        DAGSTER_DBT_SELECTOR_METADATA_KEY: selector,
    }

    def decorator(fn: Callable[..., Any]) -> AssetsDefinition:
        return multi_asset(
            name=name,
            group_name=group_name,
            can_subset=True,
            specs=asset_specs,
            check_specs=check_specs,
            op_tags=op_tags,
            partitions_def=partitions_def,
            backfill_policy=backfill_policy,
        )(fn)

    return decorator


# 1 job to 1 model generator
def build_dbt_chain_assets(
    workspace: DbtCloudWorkspace,
    *,
    select: str = DBT_DEFAULT_SELECT,
    exclude: str = DBT_DEFAULT_EXCLUDE,
    selector: str = DBT_DEFAULT_SELECTOR,
    partitions_def: dg.PartitionsDefinition,
    pool_job_ids: list[int],
    dagster_dbt_translator: DagsterDbtTranslator | None = None,
    retry_policy: dg.RetryPolicy | None = None,
    deps: Any | None = None,
) -> Sequence[AssetsDefinition]:
    """Build one AssetsDefinition per dbt model matched by `select`.

    Accepts any dbt node selection syntax (e.g. "+file_orders_refined",
    "tag:nightly", "group:orders"). Each definition is its own op —
    independent retry boundaries and one DbtCloudCliInvocation.run() per
    model-partition pair. Pool routing uses hash((partition_key, unique_id))
    so partitions spread across jobs even when multiple models run concurrently.
    """
    if not pool_job_ids:
        raise ValueError("pool_job_ids must be non-empty")

    translator = dagster_dbt_translator or DagsterDbtTranslator()
    workspace_data = workspace.get_or_fetch_workspace_data()
    manifest = workspace_data.manifest
    nodes = manifest.get("nodes", {})

    asset_specs, check_specs = build_dbt_specs(
        manifest=manifest,
        translator=translator,
        select=select,
        exclude=exclude,
        selector=selector,
        io_manager_key=None,
        project=None,
    )

    result = []
    for spec in asset_specs:
        unique_id = spec.metadata.get(DAGSTER_DBT_UNIQUE_ID_METADATA_KEY)
        if unique_id is None or unique_id not in nodes:
            continue
        node = nodes[unique_id]
        if node.get("resource_type") != "model":
            continue

        extra = {"deps": deps} if deps is not None else {}
        enriched_spec = spec.replace_attributes(
            kinds={"dbtcloud"} | spec.kinds - {"dbt"}
        ).merge_attributes(
            metadata={
                DAGSTER_DBT_CLOUD_ACCOUNT_ID_METADATA_KEY: workspace.credentials.account_id,
                DAGSTER_DBT_CLOUD_PROJECT_ID_METADATA_KEY: workspace_data.project_id,
                DAGSTER_DBT_CLOUD_ENVIRONMENT_ID_METADATA_KEY: workspace_data.environment_id,
            },
            **extra,
        )

        model_check_specs = [cs for cs in check_specs if cs.asset_key == spec.key]

        result.append(
            _build_one_dbt_asset(
                spec=enriched_spec,
                check_specs=model_check_specs,
                node=node,
                unique_id=unique_id,
                partitions_def=partitions_def,
                pool_job_ids=pool_job_ids,
                translator=translator,
                retry_policy=retry_policy,
            )
        )

    return result


def _build_one_dbt_asset(
    *,
    spec: dg.AssetSpec,
    check_specs: Sequence[dg.AssetCheckSpec],
    node: Mapping[str, Any],
    unique_id: str,
    partitions_def: dg.PartitionsDefinition,
    pool_job_ids: list[int],
    translator: DagsterDbtTranslator,
    retry_policy: dg.RetryPolicy | None,
) -> AssetsDefinition:
    fqn = ".".join(node["fqn"])
    op_name = node["name"]
    op_tags = {
        DAGSTER_DBT_SELECT_METADATA_KEY: fqn,
        DAGSTER_DBT_EXCLUDE_METADATA_KEY: "",
        DAGSTER_DBT_SELECTOR_METADATA_KEY: "",
    }

    @dg.multi_asset(
        name=op_name,
        specs=[spec],
        check_specs=check_specs,
        op_tags=op_tags,
        partitions_def=partitions_def,
        retry_policy=retry_policy,
    )
    def _dbt_model_asset(
        context: dg.AssetExecutionContext,
        dbt_cloud_workspace: DbtCloudWorkspace,
    ):
        partition_key = context.partition_key
        chosen_job_id = pool_job_ids[
            hash((partition_key, unique_id)) % len(pool_job_ids)
        ]
        context.log.info(
            f"dbt Cloud job {chosen_job_id} :: {fqn} (partition '{partition_key}')"
        )
        ws_data = dbt_cloud_workspace.get_or_fetch_workspace_data()
        invocation = DbtCloudCliInvocation.run(
            job_id=chosen_job_id,
            args=[
                "build",
                "--select",
                fqn,
                "--vars",
                json.dumps({"partition_id": partition_key}),
            ],
            client=dbt_cloud_workspace.get_client(),
            manifest=ws_data.manifest,
            dagster_dbt_translator=translator,
            context=context,
        )
        events = list(invocation.wait(timeout=DBT_CLOUD_RUN_TIMEOUT_SECONDS))
        log_compiled_sql(invocation, context)
        yield from events

    return _dbt_model_asset
