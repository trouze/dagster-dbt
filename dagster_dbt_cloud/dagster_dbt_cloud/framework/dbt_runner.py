"""Reusable dbt-Cloud-backed asset factories.

`build_dbt_chain_assets` — one AssetsDefinition per dbt model in a named
group. Each definition is its own op (independent retry boundary, one dbt
Cloud run per model). Use when per-model retry granularity matters.

For the common case of routing a dbt selection across a job pool by partition,
use @dbt_cloud_assets directly in your pipeline's assets.py — it produces the
same multi_asset with full dbt lineage, and your function body controls pool
routing. See pipelines/partition_demo/assets.py for the pattern.

Pool IDs are captured in closure at definition time. No op config needed.
"""

import json
from collections.abc import Mapping, Sequence
from typing import Any

import dagster as dg
from dagster_dbt.cloud_v2.cli_invocation import DbtCloudCliInvocation
from dagster_dbt.cloud_v2.resources import DbtCloudWorkspace
from dagster_dbt.dagster_dbt_translator import DagsterDbtTranslator

# Bump poll timeout to 30 min so runs don't fail spuriously while waiting.
DBT_CLOUD_RUN_TIMEOUT_SECONDS = 1800


# ---------------------------------------------------------------------------
# Per-model factory (fine-grained retry)
# ---------------------------------------------------------------------------

def build_dbt_chain_assets(
    workspace: DbtCloudWorkspace,
    *,
    group_name: str,
    partitions_def: dg.PartitionsDefinition,
    pool_job_ids: list[int],
    retry_policy: dg.RetryPolicy | None = None,
) -> Sequence[dg.AssetsDefinition]:
    """Build one AssetsDefinition per dbt model in `group_name`."""
    if not pool_job_ids:
        raise ValueError("pool_job_ids must be non-empty")

    translator = DagsterDbtTranslator()
    ws_data = workspace.get_or_fetch_workspace_data()
    manifest = ws_data.manifest
    nodes = manifest.get("nodes", {})

    selected_uids = [
        uid
        for uid, node in nodes.items()
        if node.get("resource_type") == "model" and node.get("group") == group_name
    ]

    return [
        _build_one_dbt_asset(
            spec=translator.get_asset_spec(manifest, uid, None),
            node=nodes[uid],
            unique_id=uid,
            partitions_def=partitions_def,
            pool_job_ids=pool_job_ids,
            translator=translator,
            retry_policy=retry_policy,
        )
        for uid in selected_uids
    ]


def _build_one_dbt_asset(
    *,
    spec: dg.AssetSpec,
    node: Mapping[str, Any],
    unique_id: str,
    partitions_def: dg.PartitionsDefinition,
    pool_job_ids: list[int],
    translator: DagsterDbtTranslator,
    retry_policy: dg.RetryPolicy | None,
) -> dg.AssetsDefinition:
    fqn = ".".join(node["fqn"])
    op_name = node["name"]

    @dg.multi_asset(
        name=op_name,
        specs=[spec],
        partitions_def=partitions_def,
        retry_policy=retry_policy,
    )
    def _dbt_model_asset(
        context: dg.AssetExecutionContext,
        dbt_cloud_workspace: DbtCloudWorkspace,
    ):
        partition_key = context.partition_key
        job_id = pool_job_ids[hash((partition_key, unique_id)) % len(pool_job_ids)]
        args = [
            "build",
            "--select",
            fqn,
            "--vars",
            json.dumps({"partition_id": partition_key}),
        ]
        context.log.info(
            f"dbt Cloud job {job_id} :: {fqn} (partition '{partition_key}')"
        )
        ws_data = dbt_cloud_workspace.get_or_fetch_workspace_data()
        invocation = DbtCloudCliInvocation.run(
            job_id=job_id,
            args=args,
            client=dbt_cloud_workspace.get_client(),
            manifest=ws_data.manifest,
            dagster_dbt_translator=translator,
            context=context,
        )
        yield from invocation.wait(timeout=DBT_CLOUD_RUN_TIMEOUT_SECONDS)

    return _dbt_model_asset


