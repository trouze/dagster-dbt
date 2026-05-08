"""Assets for the address-hygiene pipeline.

- `dbt_chain_assets`: one multi_asset covering every dbt model in the
  `acxiom_demo` group; chain1 and chain2 jobs select subsets via op config.
- `hygiene_mock`: bridge between chain1 and chain2 — calls the dbt-managed
  `address_hygiene_pending` function, runs results through a fake hygiene
  API, and inserts into `hygiene_results`.
"""

import dagster as dg
from dagster_dbt.cloud_v2.resources import DbtCloudWorkspace

from dagster_dbt_cloud.framework.dbt_runner import build_dbt_chain_assets
from dagster_dbt_cloud.framework.snowflake import (
    SnowflakeResource,
    dedupe_pending_rows,
    simulate_hygiene_api,
)
from dagster_dbt_cloud.framework.sources import get_function_location

from .partitions import file_partitions

# Asset key for the dbt model that bridges chain1 and chain2. The dbt
# translator uses the configured schema as the asset-key prefix, so
# `name_address` lives at `["partition_demo", "name_address"]`.
NAME_ADDRESS_KEY = dg.AssetKey(["partition_demo", "name_address"])

# chain1 builds everything upstream of name_address (the address_hygiene_pending
# function refs file_orders_refined and name_address; functions don't surface
# as Dagster assets, so we select through the model that anchors that scope).
chain1_selection = dg.AssetSelection.keys(NAME_ADDRESS_KEY).upstream()
# chain2 re-runs name_address incrementally after hygiene_mock has populated
# hygiene_results.
chain2_selection = dg.AssetSelection.keys(NAME_ADDRESS_KEY)


def build_dbt_assets(workspace: DbtCloudWorkspace) -> dg.AssetsDefinition:
    return build_dbt_chain_assets(
        workspace,
        name="dbt_chain_assets",
        select="group:acxiom_demo",
        partitions_def=file_partitions,
    )


@dg.asset(
    name="hygiene_mock",
    partitions_def=file_partitions,
    deps=[NAME_ADDRESS_KEY],
    description=(
        "External hygiene API mock. For a partition, calls the "
        "address_hygiene_pending function, runs results through the fake "
        "hygiene API, and inserts into hygiene_results."
    ),
)
def hygiene_mock(
    context: dg.AssetExecutionContext,
    dbt_cloud_workspace: DbtCloudWorkspace,
    snowflake: SnowflakeResource,
) -> dg.MaterializeResult:
    partition_id = context.partition_key

    db, schema = get_function_location(dbt_cloud_workspace, "address_hygiene_pending")
    sql = (
        f'SELECT * FROM TABLE("{db}"."{schema}".address_hygiene_pending(%s))'
    )
    pending = snowflake.execute(sql, (partition_id,))
    context.log.info(
        f"address_hygiene_pending returned {len(pending)} rows for partition '{partition_id}'."
    )

    deduped = dedupe_pending_rows(pending)
    results = simulate_hygiene_api(deduped)
    snowflake.insert_hygiene_results(results)

    return dg.MaterializeResult(
        metadata={
            "partition_id": partition_id,
            "pending_row_count": len(pending),
            "deduped_row_count": len(deduped),
            "inserted_row_count": len(results),
        }
    )
