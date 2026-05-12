"""Assets for the partition_demo pipeline.

  chain1              one isolated dbt job per model in "+file_orders_refined hygiene_results"
      ↓  (deps derived from chain1 asset keys)
  hygiene_mock        Python: query refined → mock API → insert hygiene_results
      ↓  (cascade sensor, cross-job)
  partition_final     dbt build name_address

chain1 routes each model-partition to a pool job via hash((partition_key, unique_id)).
Source data tests run per-model — dbt build selects the individual model fqn so
its upstream source tests are included.
"""

from collections.abc import Sequence

import dagster as dg
from dagster_dbt.cloud_v2.resources import DbtCloudWorkspace

from dagster_dbt_cloud.framework.dbt_runner import (
    build_dbt_chain_assets,
)
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
) -> Sequence[dg.AssetsDefinition]:
    """Build all pipeline assets, capturing pool_job_ids in closure.

    Returns a flat list: one AssetsDefinition per model in chain1, plus
    hygiene_mock and partition_final. Call once from build_pipeline_defs
    after pool IDs are resolved.
    """
    chain1_assets = build_dbt_chain_assets(
        workspace,
        select="+file_orders_refined hygiene_results",
        partitions_def=file_partitions,
        pool_job_ids=pool_job_ids,
    )

    chain1_keys = [key for asset in chain1_assets for key in asset.keys]

    @dg.asset(
        name="hygiene_mock",
        partitions_def=file_partitions,
        deps=chain1_keys,
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

    chain2_assets = build_dbt_chain_assets(
        workspace,
        select="name_address",
        partitions_def=file_partitions,
        pool_job_ids=pool_job_ids,
        deps=[hygiene_mock],
    )

    return [*chain1_assets, hygiene_mock, *chain2_assets]
