"""Top-level Dagster definitions.

Composes per-pipeline `Definitions` from each `pipelines/<name>` package and
merges them together with the shared resources from `framework/`.
"""

import dagster as dg

from dagster_dbt_cloud.framework.snowflake import build_snowflake_resource
from dagster_dbt_cloud.framework.workspace import dbt_cloud_workspace
from dagster_dbt_cloud.pipelines.address_hygiene.defs import (
    build_pipeline_defs as address_hygiene_defs,
)

defs = dg.Definitions.merge(
    dg.Definitions(
        resources={
            "dbt_cloud_workspace": dbt_cloud_workspace,
            "snowflake": build_snowflake_resource(),
        },
    ),
    address_hygiene_defs(dbt_cloud_workspace),
)
