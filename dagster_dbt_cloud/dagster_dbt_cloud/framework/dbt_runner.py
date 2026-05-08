"""Reusable per-model dbt-Cloud-backed asset factory.

`build_dbt_chain_assets(workspace, group_name=..., ...)` returns one
`AssetsDefinition` per dbt model in the named dbt group. Each definition is
its own op in Dagster's asset job, which means:

  - Independent retry boundaries: re-executing from failure only re-runs
    the failed model (and any downstream blocked by it).
  - One `DbtCloudCliInvocation.run()` per model, routed deterministically
    across the pool by `hash((partition_key, unique_id))`.
  - Concurrency comes from Dagster's executor running independent ops in
    parallel — no manual wave/topo logic; the asset graph handles ordering.

Pool IDs are captured in closure at definition time. No op config is needed
on the asset jobs.

Note on parse jobs: we deliberately do NOT call `load_dbt_cloud_asset_specs`
here. That helper internally instantiates a freshly-configured workspace
(via `process_config_and_initialize_cm`), so its `@cached_method` for
`fetch_workspace_data` does not share with the `workspace` we hold —
calling both would trigger two adhoc `dbt parse` runs on dbt Cloud at
definition time. Instead we fetch the manifest once via
`get_or_fetch_workspace_data()` and build specs directly with the
translator.
"""

import json
from collections.abc import Mapping, Sequence
from typing import Any

import dagster as dg
from dagster_dbt.cloud_v2.cli_invocation import DbtCloudCliInvocation
from dagster_dbt.cloud_v2.resources import DbtCloudWorkspace
from dagster_dbt.dagster_dbt_translator import DagsterDbtTranslator


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
        chosen_job_id = pool_job_ids[
            hash((partition_key, unique_id)) % len(pool_job_ids)
        ]
        args = [
            "build",
            "--select",
            fqn,
            "--vars",
            json.dumps({"partition_id": partition_key}),
        ]
        context.log.info(
            f"dbt Cloud job {chosen_job_id} :: {fqn} (partition '{partition_key}')"
        )
        ws_data = dbt_cloud_workspace.get_or_fetch_workspace_data()
        invocation = DbtCloudCliInvocation.run(
            job_id=chosen_job_id,
            args=args,
            client=dbt_cloud_workspace.get_client(),
            manifest=ws_data.manifest,
            dagster_dbt_translator=translator,
            context=context,
        )
        yield from invocation.wait()

    return _dbt_model_asset
