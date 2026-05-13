"""Definitions for the partition_demo pipeline.

Called once by the top-level definitions.py. To add a new pipeline:
  1. Copy this folder.
  2. Swap the dbt select strings in assets.py.
  3. Register the new build_pipeline_defs call in definitions.py.
"""

import dagster as dg
from dagster_dbt.cloud_v2.resources import DbtCloudWorkspace

from .assets import build_pipeline_assets


def build_pipeline_defs(workspace: DbtCloudWorkspace) -> dg.Definitions:
    return dg.Definitions(assets=list(build_pipeline_assets(workspace)))
