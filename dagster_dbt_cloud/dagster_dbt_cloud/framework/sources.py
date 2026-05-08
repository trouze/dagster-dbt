"""Look up the database/schema of dbt entities from a manifest.

Lets Dagster code stay environment-aware: dev, prod, and PR builds all land
their dbt-managed objects (sources, functions, models) at different locations,
and reading the manifest avoids hardcoding any of them in Dagster.

Each helper has two flavors: one that reads from the workspace's cached
manifest (cheap, but can be stale relative to a just-completed dbt build),
and one that takes an explicit manifest dict — useful right after a
`DbtCloudCliInvocation`, where `invocation.run_handler.get_manifest()`
gives you the fresh manifest from that run.
"""

from collections.abc import Mapping
from typing import Any

from dagster_dbt.cloud_v2.resources import DbtCloudWorkspace


def find_source_location_in_manifest(
    manifest: Mapping[str, Any],
    source_name: str,
) -> tuple[str, str]:
    for node in manifest.get("sources", {}).values():
        if node.get("source_name") == source_name:
            return node["database"], node["schema"]
    raise ValueError(f"dbt source '{source_name}' not found in manifest sources")


def find_function_location_in_manifest(
    manifest: Mapping[str, Any],
    function_name: str,
) -> tuple[str, str]:
    for node in manifest.get("functions", {}).values():
        if node.get("name") == function_name:
            return node["database"], node["schema"]
    raise ValueError(f"dbt function '{function_name}' not found in manifest functions")


def find_model_location_in_manifest(
    manifest: Mapping[str, Any],
    model_name: str,
) -> tuple[str, str, str]:
    """Return (database, schema, identifier) for a dbt model by name."""
    for node in manifest.get("nodes", {}).values():
        if node.get("resource_type") == "model" and node.get("name") == model_name:
            return node["database"], node["schema"], node.get("alias") or node["name"]
    raise ValueError(f"dbt model '{model_name}' not found in manifest nodes")


def get_source_location(
    workspace: DbtCloudWorkspace,
    source_name: str,
) -> tuple[str, str]:
    """Return (database, schema) for a dbt source by name (cached manifest)."""
    return find_source_location_in_manifest(
        workspace.get_or_fetch_workspace_data().manifest, source_name
    )


def get_function_location(
    workspace: DbtCloudWorkspace,
    function_name: str,
) -> tuple[str, str]:
    """Return (database, schema) for a dbt function by name (cached manifest)."""
    return find_function_location_in_manifest(
        workspace.get_or_fetch_workspace_data().manifest, function_name
    )
