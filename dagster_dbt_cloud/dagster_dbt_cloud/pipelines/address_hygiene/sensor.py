"""Discover new partition tables in Snowflake and register them as dynamic
partitions. The demo surfaces every discovered partition as runnable from
the UI; the sensor does not filter to "new" partitions.
"""

import dagster as dg
from dagster_dbt.cloud_v2.resources import DbtCloudWorkspace

from dagster_dbt_cloud.framework.snowflake import SnowflakeResource
from dagster_dbt_cloud.framework.sources import get_source_location

from .partitions import file_partitions

INBOUND_TABLE_PREFIX = "inbound_file_orders_"


def build_file_sensor(chain1_job) -> dg.SensorDefinition:
    @dg.sensor(
        name="partition_file_sensor",
        job=chain1_job,
        minimum_interval_seconds=60,
    )
    def partition_file_sensor(
        context: dg.SensorEvaluationContext,
        dbt_cloud_workspace: DbtCloudWorkspace,
        snowflake: SnowflakeResource,
    ) -> dg.SensorResult:
        db, schema = get_source_location(dbt_cloud_workspace, "partition_demo")
        rows = snowflake.execute(
            f'SHOW TABLES LIKE \'{INBOUND_TABLE_PREFIX}%\' IN SCHEMA "{db}"."{schema}"'
        )
        # Snowflake SHOW TABLES returns a "name" column.
        table_names = [row.get("name") for row in rows if row.get("name")]
        partition_ids = sorted(
            name[len(INBOUND_TABLE_PREFIX):]
            for name in table_names
            if name.lower().startswith(INBOUND_TABLE_PREFIX.lower())
        )

        existing = set(
            context.instance.get_dynamic_partitions(file_partitions.name)
        )
        new_partitions = [pid for pid in partition_ids if pid not in existing]

        run_requests = [
            dg.RunRequest(
                run_key=f"chain1-{pid}",
                partition_key=pid,
            )
            for pid in partition_ids
        ]

        dynamic_partitions_requests = (
            [file_partitions.build_add_request(new_partitions)]
            if new_partitions
            else []
        )

        return dg.SensorResult(
            run_requests=run_requests,
            dynamic_partitions_requests=dynamic_partitions_requests,
        )

    return partition_file_sensor
