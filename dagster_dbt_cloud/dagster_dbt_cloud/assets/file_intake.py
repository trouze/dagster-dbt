import dagster as dg

from dagster_dbt_cloud.partitions import file_partitions
from dagster_dbt_cloud.resources.snowflake import SnowflakeResource


@dg.asset(
    partitions_def=file_partitions,
    group_name="acxiom_demo",
    op_tags={"dagster/concurrency_key": "file_intake"},
)
def file_intake(
    context: dg.AssetExecutionContext,
    snowflake: SnowflakeResource,
) -> dg.MaterializeResult:
    partition_id = context.partition_key
    table = f"inbound_file_orders_{partition_id}"
    context.log.info("Mock file intake for partition %s — checking source %s.", partition_id, table)

    rows = snowflake.execute(
        f'select count(*) as row_count from "{snowflake.database}"."{snowflake.schema_name}"."{table.upper()}"'
    )
    row_count = int(rows[0]["row_count"]) if rows else 0
    context.log.info("Source table %s has %s rows.", table, row_count)

    return dg.MaterializeResult(
        metadata={
            "partition_id": partition_id,
            "source_table": f"{snowflake.database}.{snowflake.schema_name}.{table}",
            "row_count": row_count,
        }
    )
