"""Builds the Definitions for the address-hygiene pipeline.

The root `definitions.py` calls `build_pipeline_defs(...)` and merges the
result with other pipelines' definitions. Adding a new pipeline = copy this
folder, swap the dbt selector, register in `definitions.py`.
"""

import dagster as dg
from dagster_dbt.cloud_v2.resources import DbtCloudWorkspace

from dagster_dbt_cloud.framework.pool_lookup import fetch_pool_job_ids

from .assets import build_dbt_assets, hygiene_mock
from .jobs import build_chain_jobs, hygiene_job
from .sensor import build_file_sensor


def build_pipeline_defs(workspace: DbtCloudWorkspace) -> dg.Definitions:
    dbt_chain_assets = build_dbt_assets(workspace)
    pool_job_ids = fetch_pool_job_ids(workspace, name_prefix="partition_runner")
    chain1_job, chain2_job = build_chain_jobs(pool_job_ids)
    partition_file_sensor = build_file_sensor(chain1_job)

    return dg.Definitions(
        assets=[dbt_chain_assets, hygiene_mock],
        jobs=[chain1_job, chain2_job, hygiene_job],
        sensors=[partition_file_sensor],
    )
