"""Drop-in replacement for dagster_dbt.cloud_v2.dbt_cloud_assets.

Fetches the dbt Cloud manifest once per workspace instance via
workspace.get_or_fetch_workspace_data() (cached on the workspace object), then
applies dbt node selection locally using build_dbt_specs — so N decorators
sharing the same workspace trigger exactly one parse job instead of N.
"""

from collections.abc import Callable
from typing import Any

import dagster as dg
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
from dagster_dbt.cloud_v2.resources import DbtCloudWorkspace
from dagster_dbt.dagster_dbt_translator import DagsterDbtTranslator

DBT_CLOUD_RUN_TIMEOUT_SECONDS = 1800


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
    deps: dg.CoercibleToAssetDepsDefinition | None = None,
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
            deps=deps,
        )(fn)

    return decorator
