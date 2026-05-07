import dagster as dg

from dagster_dbt_cloud.partitions import FILE_PARTITIONS_NAME, file_partitions
from dagster_dbt_cloud.resources.snowflake import SnowflakeResource

DEMO_PIPELINE_JOB_NAME = "acxiom_demo_pipeline"
SOURCE_TABLE_PREFIX = "INBOUND_FILE_ORDERS_"


@dg.sensor(
    job_name=DEMO_PIPELINE_JOB_NAME,
    minimum_interval_seconds=60,
    default_status=dg.DefaultSensorStatus.STOPPED,
)
def file_partition_sensor(
    context: dg.SensorEvaluationContext,
    snowflake: SnowflakeResource,
) -> dg.SensorResult:
    rows = snowflake.execute(
        f"""
        select table_name
        from "{snowflake.database}".information_schema.tables
        where table_schema = upper(%s)
          and table_name ilike %s
        """,
        params=(snowflake.schema_name, f"{SOURCE_TABLE_PREFIX}%"),
    )

    discovered: set[str] = set()
    for row in rows:
        name = str(row.get("table_name", "")).upper()
        if not name.startswith(SOURCE_TABLE_PREFIX):
            continue
        suffix = name[len(SOURCE_TABLE_PREFIX) :]
        if suffix:
            discovered.add(suffix.lower())

    existing = set(context.instance.get_dynamic_partitions(FILE_PARTITIONS_NAME))
    new = sorted(discovered - existing)

    if not new:
        return dg.SensorResult(skip_reason="No new file partitions discovered.")

    context.log.info("Discovered new file partitions: %s", new)
    return dg.SensorResult(
        run_requests=[dg.RunRequest(partition_key=p) for p in new],
        dynamic_partitions_requests=[file_partitions.build_add_request(new)],
    )
