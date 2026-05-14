"""Mock external hygiene-API call between two dbt stages.

Reads partitioned rows from `file_orders_refined`, runs them through a mock
hygiene service, and writes results into `hygiene_results`. Sits between the
chain1 dbt stage (which produces both source tables) and the chain2 dbt stage
(`name_address`), bridging them via asset-key deps.
"""

import dagster as dg
from dagster_dbt.cloud_v2.resources import DbtCloudWorkspace

from dagster_dbt_cloud.framework.sources import get_model_location
from dagster_dbt_cloud.resources.snowflake import (
    SnowflakeResource,
    dedupe_pending_rows,
    simulate_hygiene_api,
)

file_partitions = dg.StaticPartitionsDefinition(["001", "002", "003"])


@dg.asset(
    name="hygiene_mock",
    partitions_def=file_partitions,
    deps=[
        dg.AssetKey(["partition_demo", "file_orders_refined"]),
        dg.AssetKey(["partition_demo", "hygiene_results"]),
    ],
    description=(
        "Queries file_orders_refined for the partition, sends eligible records "
        "through the mock hygiene API, and inserts results into hygiene_results."
    ),
)
def hygiene_mock(
    context: dg.AssetExecutionContext,
    dbt_cloud_workspace: DbtCloudWorkspace,
    snowflake: SnowflakeResource,
) -> dg.MaterializeResult:
    partition_id = context.partition_key

    db, schema, identifier = get_model_location(
        dbt_cloud_workspace, "file_orders_refined"
    )
    pending = snowflake.execute(
        f"SELECT order_id, customer_id FROM {db}.{schema}.{identifier} WHERE file_id = %s",
        (partition_id,),
    )
    context.log.info(
        f"file_orders_refined returned {len(pending)} rows for partition '{partition_id}'."
    )

    deduped = dedupe_pending_rows(pending)
    results = simulate_hygiene_api(deduped)

    hr_db, hr_schema, hr_id = get_model_location(dbt_cloud_workspace, "hygiene_results")
    snowflake.insert_hygiene_results(
        results, target_relation=f"{hr_db}.{hr_schema}.{hr_id}"
    )
    context.log.info(f"Inserted {len(results)} hygiene results.")

    return dg.MaterializeResult(
        metadata={
            "partition_id": partition_id,
            "pending_row_count": len(pending),
            "deduped_row_count": len(deduped),
            "inserted_row_count": len(results),
        }
    )
