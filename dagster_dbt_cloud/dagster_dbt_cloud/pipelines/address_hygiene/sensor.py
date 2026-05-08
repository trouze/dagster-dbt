"""Discover new partition tables in Snowflake and register them as dynamic
partitions. The demo surfaces every discovered partition as runnable from
the UI; the sensor does not filter to "new" partitions.

Also defines the cascade sensors that chain chain1 → hygiene_mock → chain2
per partition. Asset deps already encode the order; jobs don't auto-cascade,
so we watch for run success and fire the next stage with the same partition.
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
        # Identifiers unquoted: matches dbt's default Snowflake quoting policy
        # and lets Snowflake case-fold to the actual stored identifier.
        rows = snowflake.execute(
            f"SHOW TABLES LIKE '{INBOUND_TABLE_PREFIX}%' IN SCHEMA {db}.{schema}"
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


def build_cascade_sensors(
    chain1_job: dg.JobDefinition,
    hygiene_job: dg.JobDefinition,
    chain2_job: dg.JobDefinition,
) -> tuple[dg.SensorDefinition, dg.SensorDefinition]:
    """Cascade chain1 → hygiene_mock → chain2 per partition on success."""

    @dg.run_status_sensor(
        name="partition_chain1_to_hygiene",
        run_status=dg.DagsterRunStatus.SUCCESS,
        monitored_jobs=[chain1_job],
        request_job=hygiene_job,
    )
    def chain1_to_hygiene(context: dg.RunStatusSensorContext):
        pk = context.dagster_run.tags.get("dagster/partition")
        if not pk:
            return
        return dg.RunRequest(run_key=f"hygiene-{pk}", partition_key=pk)

    @dg.run_status_sensor(
        name="partition_hygiene_to_chain2",
        run_status=dg.DagsterRunStatus.SUCCESS,
        monitored_jobs=[hygiene_job],
        request_job=chain2_job,
    )
    def hygiene_to_chain2(context: dg.RunStatusSensorContext):
        pk = context.dagster_run.tags.get("dagster/partition")
        if not pk:
            return
        return dg.RunRequest(run_key=f"chain2-{pk}", partition_key=pk)

    return chain1_to_hygiene, hygiene_to_chain2
