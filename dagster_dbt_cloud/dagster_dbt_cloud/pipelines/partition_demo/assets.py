"""Assets for the partition_demo pipeline.

  partition_series1   dbt build +file_orders_refined hygiene_results
      ↓  (deps declared via AssetsDefinition reference)
  hygiene_mock        Python: query refined → mock API → insert hygiene_results
      ↓  (cascade sensor, cross-job)
  partition_final     dbt build name_address

Both dbt stages route partitions to pool jobs via hash(partition_key) % N.
Source data tests run implicitly — dbt build with the + selector tests upstream
sources before building each model, so no separate preflight step is needed.
"""

import json

import dagster as dg
from dagster_dbt.cloud_v2.cli_invocation import DbtCloudCliInvocation
from dagster_dbt.cloud_v2.resources import DbtCloudWorkspace
from dagster_dbt.dagster_dbt_translator import DagsterDbtTranslator

from dagster_dbt_cloud.framework.dbt_runner import DBT_CLOUD_RUN_TIMEOUT_SECONDS, dbt_cloud_assets
from dagster_dbt_cloud.framework.sources import get_model_location
from dagster_dbt_cloud.resources.snowflake import (
    SnowflakeResource,
    dedupe_pending_rows,
    simulate_hygiene_api,
)

from .partitions import file_partitions


def build_pipeline_assets(
    workspace: DbtCloudWorkspace,
    pool_job_ids: list[int],
) -> tuple[dg.AssetsDefinition, dg.AssetsDefinition, dg.AssetsDefinition]:
    """Build the three pipeline assets, capturing pool_job_ids in closure.

    Returns (partition_series1, hygiene_mock, partition_final).
    Call once from build_pipeline_defs after pool IDs are resolved.
    """
    translator = DagsterDbtTranslator()

    @dbt_cloud_assets(
        workspace=workspace,
        select="+file_orders_refined hygiene_results",
        partitions_def=file_partitions,
    )
    def partition_series1(
        context: dg.AssetExecutionContext,
        dbt_cloud_workspace: DbtCloudWorkspace,
    ):
        partition_key = context.partition_key
        job_id = pool_job_ids[hash(partition_key) % len(pool_job_ids)]
        ws_data = dbt_cloud_workspace.get_or_fetch_workspace_data()
        invocation = DbtCloudCliInvocation.run(
            job_id=job_id,
            args=[
                "build",
                "--select", "+file_orders_refined hygiene_results",
                "--vars", json.dumps({"partition_id": partition_key}),
            ],
            client=dbt_cloud_workspace.get_client(),
            manifest=ws_data.manifest,
            dagster_dbt_translator=translator,
            context=context,
        )
        yield from invocation.wait(timeout=DBT_CLOUD_RUN_TIMEOUT_SECONDS)

    @dg.asset(
        name="hygiene_mock",
        partitions_def=file_partitions,
        deps=[partition_series1],
        description=(
            "Queries file_orders_refined for the partition, sends eligible records "
            "through the mock hygiene API, and inserts results into hygiene_results. "
            "Bridges series1 (dbt build) and final (name_address build)."
        ),
    )
    def hygiene_mock(
        context: dg.AssetExecutionContext,
        dbt_cloud_workspace: DbtCloudWorkspace,
        snowflake: SnowflakeResource,
    ) -> dg.MaterializeResult:
        partition_id = context.partition_key

        db, schema, identifier = get_model_location(dbt_cloud_workspace, "file_orders_refined")
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
        snowflake.insert_hygiene_results(results, target_relation=f"{hr_db}.{hr_schema}.{hr_id}")
        context.log.info(f"Inserted {len(results)} hygiene results.")

        return dg.MaterializeResult(
            metadata={
                "partition_id": partition_id,
                "pending_row_count": len(pending),
                "deduped_row_count": len(deduped),
                "inserted_row_count": len(results),
            }
        )

    @dbt_cloud_assets(
        workspace=workspace,
        select="name_address",
        partitions_def=file_partitions,
        deps=[hygiene_mock],
    )
    def partition_final(
        context: dg.AssetExecutionContext,
        dbt_cloud_workspace: DbtCloudWorkspace,
    ):
        partition_key = context.partition_key
        job_id = pool_job_ids[hash(partition_key) % len(pool_job_ids)]
        ws_data = dbt_cloud_workspace.get_or_fetch_workspace_data()
        invocation = DbtCloudCliInvocation.run(
            job_id=job_id,
            args=[
                "build",
                "--select", "name_address",
                "--vars", json.dumps({"partition_id": partition_key}),
            ],
            client=dbt_cloud_workspace.get_client(),
            manifest=ws_data.manifest,
            dagster_dbt_translator=translator,
            context=context,
        )
        yield from invocation.wait(timeout=DBT_CLOUD_RUN_TIMEOUT_SECONDS)

    return partition_series1, hygiene_mock, partition_final
