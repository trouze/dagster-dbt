"""Drop-in replacement for dagster_dbt.cloud_v2.dbt_cloud_assets.

Fetches the dbt Cloud manifest once per workspace instance via
workspace.get_or_fetch_workspace_data() (cached on the workspace object), then
applies dbt node selection locally using build_dbt_specs — so N decorators
sharing the same workspace trigger exactly one parse job instead of N.

Also provides run_or_retry_dbt, a generator that routes first attempts through
a fresh dbt Cloud job trigger and retries through dbt Cloud's /retry/ API so
that only failed models (and their downstream deps) re-run — not the full
selection.
"""

from collections.abc import Callable, Iterator, Mapping, Sequence
from typing import Any

import dagster as dg
import httpx
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
    DBT_DEFAULT_EXCLUDE,
    DBT_DEFAULT_SELECT,
    DBT_DEFAULT_SELECTOR,
    build_dbt_specs,
)
from dagster_dbt.cloud_v2.cli_invocation import DbtCloudCliInvocation
from dagster_dbt.cloud_v2.run_handler import DbtCloudJobRunHandler
from dagster_dbt.cloud_v2.resources import DbtCloudWorkspace
from dagster_dbt.dagster_dbt_translator import DagsterDbtTranslator

DBT_CLOUD_RUN_TIMEOUT_SECONDS = 1800

_DBT_RUN_ID_TAG = "dagster_dbt_cloud/retry_run_id"


# ---------------------------------------------------------------------------
# Retry helpers
# ---------------------------------------------------------------------------

def _store_dbt_run_id(context: dg.AssetExecutionContext, run_id: int) -> None:
    tag_key = f"{_DBT_RUN_ID_TAG}/{context.op_def.name}"
    context.instance.add_run_tags(context.run_id, {tag_key: str(run_id)})


def _get_stored_dbt_run_id(context: dg.AssetExecutionContext) -> int | None:
    tag_key = f"{_DBT_RUN_ID_TAG}/{context.op_def.name}"
    run = context.instance.get_run_by_id(context.run_id)
    value = (run.tags or {}).get(tag_key)
    return int(value) if value else None


def _call_dbt_retry_api(
    run_id: int,
    account_id: int | str,
    access_url: str,
    token: str,
    timeout: int,
) -> int:
    url = f"{access_url.rstrip('/')}/api/v2/accounts/{account_id}/runs/{run_id}/retry/"
    resp = httpx.post(
        url,
        headers={"Authorization": f"Token {token}"},
        timeout=timeout,
    )
    resp.raise_for_status()
    return int(resp.json()["data"]["id"])


def run_or_retry_dbt(
    context: dg.AssetExecutionContext,
    *,
    job_id: int,
    workspace: DbtCloudWorkspace,
    manifest: Mapping[str, Any],
    translator: DagsterDbtTranslator,
    args: Sequence[str],
    timeout: float = DBT_CLOUD_RUN_TIMEOUT_SECONDS,
) -> Iterator:
    """Trigger a dbt Cloud run or retry the previous failed run from its failure point.

    On the first attempt: triggers a fresh run, stores the dbt Cloud run_id in
    Dagster's run tags (DB-backed — survives process boundaries between retries).

    On retry attempts: reads the stored run_id, calls the dbt Cloud /retry/ API
    so that only the failed models and their downstream dependencies re-run, then
    polls and streams events exactly like a normal run.

    Each retry overwrites the stored run_id so chained retries always re-enter
    from the most recent failure point.
    """
    client = workspace.get_client()

    if context.retry_number > 0:
        failed_run_id = _get_stored_dbt_run_id(context)
        if failed_run_id:
            context.log.info(
                f"Retry attempt {context.retry_number} — calling dbt Cloud retry API "
                f"for run {failed_run_id}."
            )
            new_run_id = _call_dbt_retry_api(
                run_id=failed_run_id,
                account_id=workspace.credentials.account_id,
                access_url=workspace.credentials.access_url,
                token=workspace.credentials.token,
                timeout=workspace.request_timeout,
            )
            context.log.info(f"dbt Cloud retry dispatched — new run: {new_run_id}")
            _store_dbt_run_id(context, new_run_id)
            handler = DbtCloudJobRunHandler(
                job_id=job_id,
                run_id=new_run_id,
                args=args,
                client=client,
            )
            invocation = DbtCloudCliInvocation(
                args=args,
                client=client,
                manifest=manifest,
                dagster_dbt_translator=translator,
                run_handler=handler,
                context=context,
            )
            yield from invocation.wait(timeout=timeout)
            return

    invocation = DbtCloudCliInvocation.run(
        job_id=job_id,
        args=args,
        client=client,
        manifest=manifest,
        dagster_dbt_translator=translator,
        context=context,
    )
    _store_dbt_run_id(context, invocation.run_handler.run_id)
    yield from invocation.wait(timeout=timeout)


# ---------------------------------------------------------------------------
# Asset decorator
# ---------------------------------------------------------------------------

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

    asset_specs = [
        spec.replace_attributes(kinds={"dbtcloud"} | spec.kinds - {"dbt"}).merge_attributes(
            metadata={
                DAGSTER_DBT_CLOUD_ACCOUNT_ID_METADATA_KEY: workspace.credentials.account_id,
                DAGSTER_DBT_CLOUD_PROJECT_ID_METADATA_KEY: workspace_data.project_id,
                DAGSTER_DBT_CLOUD_ENVIRONMENT_ID_METADATA_KEY: workspace_data.environment_id,
            }
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
