"""Top-level Dagster definitions.

Merges shared infrastructure (resources, dbt Cloud polling sensor) with
per-pipeline Definitions. To add a new pipeline, import its
build_pipeline_defs and merge it here.
"""

import dagster as dg

from dagster_dbt_cloud.resources.dbt import dbt_cloud_polling_sensor, workspace
from dagster_dbt_cloud.resources.snowflake import build_snowflake_resource
from dagster_dbt_cloud.pipelines.partition_demo.defs import (
    build_pipeline_defs as partition_demo_defs,
)

defs = dg.Definitions.merge(
    dg.Definitions(
        resources={
            "dbt_cloud_workspace": workspace,
            "snowflake": build_snowflake_resource(),
        },
        sensors=[dbt_cloud_polling_sensor],
    ),
    partition_demo_defs(workspace),
)
