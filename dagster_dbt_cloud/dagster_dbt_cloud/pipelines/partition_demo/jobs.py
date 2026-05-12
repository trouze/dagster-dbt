"""Job factory for the partition_demo pipeline.

Jobs are built after assets because the @dbt_cloud_assets-decorated definitions
are created inside build_pipeline_assets (to close over pool_job_ids), so they
aren't importable at module level. build_jobs receives the resolved definitions
and builds asset selections from them directly.
"""

import dagster as dg

from .partitions import file_partitions


def build_jobs(
    series1_asset: dg.AssetsDefinition,
    hygiene_asset: dg.AssetsDefinition,
    final_asset: dg.AssetsDefinition,
) -> tuple[dg.JobDefinition, dg.JobDefinition, dg.JobDefinition]:
    chain1_job = dg.define_asset_job(
        name="partition_chain1",
        selection=dg.AssetSelection.assets(series1_asset),
        partitions_def=file_partitions,
        description="Builds series-1 dbt models for a partition (source tests included via dbt build).",
    )

    hygiene_job = dg.define_asset_job(
        name="partition_hygiene",
        selection=dg.AssetSelection.assets(hygiene_asset),
        partitions_def=file_partitions,
        description="Queries refined records, calls the mock hygiene API, inserts into hygiene_results.",
    )

    chain2_job = dg.define_asset_job(
        name="partition_chain2",
        selection=dg.AssetSelection.assets(final_asset),
        partitions_def=file_partitions,
        description="Incrementally rebuilds name_address after hygiene results are loaded.",
    )

    return chain1_job, hygiene_job, chain2_job
