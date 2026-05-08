"""Builds the Definitions for the address-hygiene pipeline.

The root `definitions.py` calls `build_pipeline_defs(...)` and merges the
result with other pipelines' definitions. Adding a new pipeline = copy this
folder, swap the dbt selector, register in `definitions.py`.
"""

import dagster as dg
from dagster_dbt.cloud_v2.resources import DbtCloudWorkspace

from dagster_dbt_cloud.framework.pool_lookup import fetch_pool_job_ids

from .assets import address_hygiene_pending_function, build_dbt_assets, hygiene_mock
from .jobs import build_chain_jobs, hygiene_job
from .sensor import build_cascade_sensors, build_file_sensor


def build_pipeline_defs(workspace: DbtCloudWorkspace) -> dg.Definitions:
    pool_job_ids = fetch_pool_job_ids(workspace, name_prefix="partition_runner")
    dbt_chain_assets = build_dbt_assets(workspace, pool_job_ids=pool_job_ids)
    chain1_job, chain2_job = build_chain_jobs()
    partition_file_sensor = build_file_sensor(chain1_job)
    chain1_to_hygiene, hygiene_to_chain2 = build_cascade_sensors(
        chain1_job, hygiene_job, chain2_job
    )

    return dg.Definitions(
        assets=[*dbt_chain_assets, address_hygiene_pending_function, hygiene_mock],
        jobs=[chain1_job, chain2_job, hygiene_job],
        sensors=[partition_file_sensor, chain1_to_hygiene, hygiene_to_chain2],
    )

    # --- Option B (fallback): one combined job covering all three stages ---
    # Asset deps drive ordering; the file sensor targets the combined job and
    # one run per partition executes chain1 → hygiene_mock → chain2 end-to-end.
    # Trade-off: loses ability to re-run just chain2 from the UI without
    # invoking the whole partition.
    #
    # from .assets import chain1_selection, chain2_selection
    #
    # partition_pipeline_job = dg.define_asset_job(
    #     name="partition_pipeline",
    #     selection=(
    #         chain1_selection
    #         | dg.AssetSelection.keys("hygiene_mock")
    #         | chain2_selection
    #     ),
    #     partitions_def=file_partitions,
    # )
    # partition_file_sensor = build_file_sensor(partition_pipeline_job)
    #
    # return dg.Definitions(
    #     assets=[*dbt_chain_assets, hygiene_mock],
    #     jobs=[partition_pipeline_job],
    #     sensors=[partition_file_sensor],
    # )
