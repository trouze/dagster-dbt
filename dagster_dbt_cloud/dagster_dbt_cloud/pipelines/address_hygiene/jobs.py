"""Per-chain asset jobs for the address-hygiene pipeline.

Each chain selects a subset of the per-model dbt assets; Dagster runs each
selected asset as its own op, so per-model retry from failure is automatic.
Pool routing is captured in each asset's closure at definition time, so the
jobs need no run config.
"""

import dagster as dg

from .assets import chain1_selection, chain2_selection
from .partitions import file_partitions


def build_chain_jobs():
    chain1_job = dg.define_asset_job(
        name="partition_chain1",
        selection=chain1_selection,
        partitions_def=file_partitions,
    )
    chain2_job = dg.define_asset_job(
        name="partition_chain2",
        selection=chain2_selection,
        partitions_def=file_partitions,
    )
    return chain1_job, chain2_job


hygiene_job = dg.define_asset_job(
    name="partition_hygiene",
    selection=dg.AssetSelection.keys("hygiene_mock"),
    partitions_def=file_partitions,
)
