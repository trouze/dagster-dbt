"""Top-level Dagster definitions.

Auto-loads every component / Python defs file under `dagster_dbt_cloud.defs`,
then layers in the shared resources used by the Python steps.

To add a new pipeline, create a new folder under `defs/` with one or more
`defs.yaml` files (for dbt stages) and any `.py` files for Python steps. No
edits to this file required.
"""

import dagster as dg

import dagster_dbt_cloud.defs as defs_module
from dagster_dbt_cloud.resources.github import build_github_resource
from dagster_dbt_cloud.resources.snowflake import build_snowflake_resource
from dagster_dbt_cloud.resources.workspace import build_workspace

defs = dg.Definitions.merge(
    dg.load_defs(defs_root=defs_module),
    dg.Definitions(
        resources={
            "dbt_cloud_workspace": build_workspace(),
            "snowflake": build_snowflake_resource(),
            "github": build_github_resource(),
        },
    ),
)
