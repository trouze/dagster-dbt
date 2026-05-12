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

POOL_JOB_PREFIX = "partition_runner"


def build_pipeline_defs(workspace: DbtCloudWorkspace) -> dg.Definitions:
    pool_job_ids = fetch_pool_job_ids(workspace, name_prefix=POOL_JOB_PREFIX)
    series1, hygiene_mock, final = build_pipeline_assets(workspace, pool_job_ids)
    return dg.Definitions(assets=[series1, hygiene_mock, final])
