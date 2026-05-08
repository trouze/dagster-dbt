"""Per-chain asset jobs for the address-hygiene pipeline.

Both chains drive the same `dbt_chain_assets` multi_asset; they differ only in
which assets they select. The dbt `--select` is derived inside the multi_asset
from `context.selected_asset_keys`, so the only op config each job needs is
the pool of dbt Cloud job IDs to fan partition runs across.
"""

import dagster as dg

from .assets import chain1_selection, chain2_selection
from .partitions import file_partitions


def build_chain_jobs(pool_job_ids: list[int]):
    pool_config = dg.RunConfig(
        ops={
            "dbt_chain_assets": {
                "config": {"pool_job_ids": pool_job_ids},
            }
        }
    )

    chain1_job = dg.define_asset_job(
        name="partition_chain1",
        selection=chain1_selection,
        partitions_def=file_partitions,
        config=pool_config,
    )

    chain2_job = dg.define_asset_job(
        name="partition_chain2",
        selection=chain2_selection,
        partitions_def=file_partitions,
        config=pool_config,
    )

    return chain1_job, chain2_job


hygiene_job = dg.define_asset_job(
    name="partition_hygiene",
    selection=dg.AssetSelection.keys("hygiene_mock"),
    partitions_def=file_partitions,
)
