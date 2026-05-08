"""Per-chain asset jobs for the address-hygiene pipeline.

Both chains drive the same `dbt_chain_assets` multi_asset; they differ only in
which assets they select and what op config they pass (the dbt --select arg
and the pool of dbt Cloud job IDs to fan out across).
"""

import dagster as dg

from .assets import chain1_selection, chain2_selection
from .partitions import file_partitions


def build_chain_jobs(pool_job_ids: list[int]):
    chain1_job = dg.define_asset_job(
        name="partition_chain1",
        selection=chain1_selection,
        partitions_def=file_partitions,
        config=dg.RunConfig(
            ops={
                "dbt_chain_assets": {
                    "config": {
                        "select": "+address_hygiene_pending",
                        "pool_job_ids": pool_job_ids,
                    }
                }
            }
        ),
    )

    chain2_job = dg.define_asset_job(
        name="partition_chain2",
        selection=chain2_selection,
        partitions_def=file_partitions,
        config=dg.RunConfig(
            ops={
                "dbt_chain_assets": {
                    "config": {
                        "select": "name_address",
                        "pool_job_ids": pool_job_ids,
                    }
                }
            }
        ),
    )

    return chain1_job, chain2_job


hygiene_job = dg.define_asset_job(
    name="partition_hygiene",
    selection=dg.AssetSelection.keys("hygiene_mock"),
    partitions_def=file_partitions,
)
