import dagster as dg

from dagster_dbt_cloud.partitions import file_partitions
from dagster_dbt_cloud.resources.snowflake import (
    SnowflakeResource,
    call_hygiene_api_with_retry,
    dedupe_pending_rows,
)


@dg.asset(
    partitions_def=file_partitions,
    deps=[dg.AssetKey(["file_orders_refined"])],
    group_name="acxiom_demo",
    op_tags={"dagster/concurrency_key": "hygiene_api"},
)
def address_hygiene_external(
    context: dg.AssetExecutionContext,
    snowflake: SnowflakeResource,
) -> dg.MaterializeResult:
    partition_id = context.partition_key

    udf_relation = (
        f'"{snowflake.database}"."{snowflake.schema_name}"."ADDRESS_HYGIENE_PENDING"'
    )
    pending = snowflake.execute(
        f"select * from table({udf_relation}(%s, %s))",
        params=(partition_id, 18),
    )

    if not pending:
        context.log.info("No pending hygiene records for partition %s.", partition_id)
        return dg.MaterializeResult(
            metadata={"partition_id": partition_id, "records_processed": 0}
        )

    deduped = dedupe_pending_rows(pending)
    context.log.info(
        "Calling external hygiene system for partition %s (pending=%s, deduped=%s).",
        partition_id,
        len(pending),
        len(deduped),
    )
    results = call_hygiene_api_with_retry(deduped)
    snowflake.insert_hygiene_results(results)
    context.log.info(
        "Inserted %s hygiene records for partition %s.", len(results), partition_id
    )

    return dg.MaterializeResult(
        metadata={
            "partition_id": partition_id,
            "pending_records": len(pending),
            "deduped_records": len(deduped),
            "records_processed": len(results),
        }
    )
