"""Definitions for the partition_demo pipeline.

Called once by the top-level definitions.py. To add a new pipeline:
  1. Copy this folder.
  2. Swap the dbt select strings in assets.py.
  3. Register the new build_pipeline_defs call in definitions.py.
"""

import dagster as dg
from dagster_dbt.cloud_v2.resources import DbtCloudWorkspace

from dagster_dbt_cloud.framework.pool_lookup import fetch_pool_job_ids

from .assets import build_pipeline_assets
from .jobs import build_jobs
from .sensor import build_cascade_sensors, build_partition_sensor

POOL_JOB_PREFIX = "partition_runner"


def build_pipeline_defs(workspace: DbtCloudWorkspace) -> dg.Definitions:
    # Warm the manifest cache once so all subsequent calls in this process hit it.
    workspace.get_or_fetch_workspace_data()

    pool_job_ids = fetch_pool_job_ids(workspace, name_prefix=POOL_JOB_PREFIX)

    series1, hygiene_mock, final = build_pipeline_assets(workspace, pool_job_ids)
    chain1_job, hygiene_job, chain2_job = build_jobs(series1, hygiene_mock, final)

    partition_sensor = build_partition_sensor(chain1_job)
    chain1_to_hygiene, hygiene_to_chain2 = build_cascade_sensors(
        chain1_job, hygiene_job, chain2_job
    )

    return dg.Definitions(
        assets=[series1, hygiene_mock, final],
        jobs=[chain1_job, hygiene_job, chain2_job],
        sensors=[partition_sensor, chain1_to_hygiene, hygiene_to_chain2],
    )
