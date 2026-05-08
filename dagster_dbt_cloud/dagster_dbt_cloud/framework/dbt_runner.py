"""Reusable dbt-Cloud-backed multi_asset factory.

A pipeline calls `build_dbt_chain_assets(workspace, name, select, partitions_def)`
to get a single `multi_asset` covering its dbt scope. Multiple `define_asset_job`s
in that pipeline can then select subsets via `can_subset=True`, each passing
its own dbt --select and pool of dbt Cloud job IDs through op config.

Using `DbtCloudCliInvocation.run()` directly bypasses the workspace adhoc-job
constraint and lets us route each partition to a specific job in the pool.
"""

import json
from collections.abc import Sequence

import dagster as dg
from dagster_dbt.cloud_v2.cli_invocation import DbtCloudCliInvocation
from dagster_dbt.cloud_v2.resources import DbtCloudWorkspace, load_dbt_cloud_asset_specs


class DbtChainConfig(dg.Config):
    select: str
    pool_job_ids: list[int]


def build_dbt_chain_assets(
    workspace: DbtCloudWorkspace,
    *,
    name: str,
    select: str,
    partitions_def: dg.PartitionsDefinition,
) -> dg.AssetsDefinition:
    """Build one multi_asset covering every dbt asset selected by `select`.

    `select` is the dbt selector that defines the pipeline's scope (e.g.
    `group:acxiom_demo`). Per-job behavior (the dbt --select arg used at run
    time and the pool of dbt Cloud job IDs to fan across) is supplied through
    op config on each `define_asset_job`.
    """
    specs: Sequence[dg.AssetSpec] = load_dbt_cloud_asset_specs(
        workspace=workspace,
        select=select,
    )

    @dg.multi_asset(
        name=name,
        specs=specs,
        can_subset=True,
        partitions_def=partitions_def,
    )
    def _dbt_chain_assets(
        context: dg.AssetExecutionContext,
        config: DbtChainConfig,
        dbt_cloud_workspace: DbtCloudWorkspace,
    ):
        partition_key = context.partition_key
        # Deterministic spread: same partition lands on the same pool job each
        # time (helps when re-running for debugging); collisions across
        # partitions queue on dbt Cloud, which is acceptable for the demo.
        chosen_job_id = config.pool_job_ids[
            hash(partition_key) % len(config.pool_job_ids)
        ]

        args: list[str] = [
            "build",
            "--select",
            config.select,
            "--vars",
            json.dumps({"partition_id": partition_key}),
        ]

        ws_data = dbt_cloud_workspace.get_or_fetch_workspace_data()

        context.log.info(
            f"Running dbt Cloud job {chosen_job_id} for partition '{partition_key}' "
            f"with args: {args}"
        )

        invocation = DbtCloudCliInvocation.run(
            job_id=chosen_job_id,
            args=args,
            client=dbt_cloud_workspace.get_client(),
            manifest=ws_data.manifest,
            dagster_dbt_translator=_default_translator(),
            context=context,
        )
        yield from invocation.wait()

    return _dbt_chain_assets


def _default_translator():
    from dagster_dbt.dagster_dbt_translator import DagsterDbtTranslator

    return DagsterDbtTranslator()
