"""Look up the database/schema of dbt entities from the cached manifest.

Lets Dagster code stay environment-aware: dev, prod, and PR builds all land
their dbt-managed objects (sources, functions, models) at different locations,
and reading the manifest avoids hardcoding any of them in Dagster.
"""

from dagster_dbt.cloud_v2.resources import DbtCloudWorkspace


def get_source_location(
    workspace: DbtCloudWorkspace,
    source_name: str,
) -> tuple[str, str]:
    """Return (database, schema) for a dbt source by name."""
    manifest = workspace.get_or_fetch_workspace_data().manifest
    for node in manifest.get("sources", {}).values():
        if node.get("source_name") == source_name:
            return node["database"], node["schema"]
    raise ValueError(f"dbt source '{source_name}' not found in manifest sources")


def get_function_location(
    workspace: DbtCloudWorkspace,
    function_name: str,
) -> tuple[str, str]:
    """Return (database, schema) for a dbt function by name.

    `functions` is a Fusion-specific top-level manifest section; each entry
    has its own `database` / `schema` resolved by dbt for the active target.
    """
    manifest = workspace.get_or_fetch_workspace_data().manifest
    for node in manifest.get("functions", {}).values():
        if node.get("name") == function_name:
            return node["database"], node["schema"]
    raise ValueError(f"dbt function '{function_name}' not found in manifest functions")
