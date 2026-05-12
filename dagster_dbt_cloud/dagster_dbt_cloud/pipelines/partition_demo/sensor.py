"""Sensors for the partition_demo pipeline.

partition_file_sensor   — fires chain1 for each static partition (once per cursor epoch).
                          Reset the cursor in the Dagster UI to re-trigger all partitions.
chain1_to_hygiene       — on chain1 SUCCESS, fires partition_hygiene for the same partition.
hygiene_to_chain2       — on partition_hygiene SUCCESS, fires chain2 for the same partition.

The cascade pattern keeps each stage in its own Dagster run so failures can
be retried at the right granularity (e.g. re-run just chain2 without repeating
the full dbt build).
"""

import dagster as dg

from .partitions import file_partitions


def build_partition_sensor(chain1_job: dg.JobDefinition) -> dg.SensorDefinition:
    """Sensor that triggers chain1 for every static partition.

    Uses cursor to track which partition_ids have already been submitted.
    Reset the sensor cursor in the Dagster UI to re-trigger all partitions.
    To extend to dynamic partitions, swap StaticPartitionsDefinition for
    DynamicPartitionsDefinition and add a table-discovery step here.
    """

    @dg.sensor(
        name="partition_file_sensor",
        job=chain1_job,
        minimum_interval_seconds=60,
    )
    def partition_file_sensor(
        context: dg.SensorEvaluationContext,
    ) -> dg.SensorResult:
        triggered = set(context.cursor.split(",")) if context.cursor else set()
        partition_keys = file_partitions.get_partition_keys()
        new_partitions = [pid for pid in partition_keys if pid not in triggered]

        if not new_partitions:
            return dg.SkipReason("All static partitions have already been triggered.")

        requests = [
            dg.RunRequest(run_key=f"chain1-{pid}", partition_key=pid)
            for pid in new_partitions
        ]
        context.update_cursor(",".join(sorted(triggered | set(new_partitions))))
        return dg.SensorResult(run_requests=requests)

    return partition_file_sensor


def build_cascade_sensors(
    chain1_job: dg.JobDefinition,
    hygiene_job: dg.JobDefinition,
    chain2_job: dg.JobDefinition,
) -> tuple[dg.SensorDefinition, dg.SensorDefinition]:
    """Wire chain1 → hygiene → chain2 cascade per partition on SUCCESS."""

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
