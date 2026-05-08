"""Reusable dbt-Cloud-backed multi_asset factory.

A pipeline calls `build_dbt_chain_assets(workspace, name, select, partitions_def)`
to get a single `multi_asset` covering its dbt scope. Multiple `define_asset_job`s
in that pipeline can then select subsets via `can_subset=True`, each passing
its own pool of dbt Cloud job IDs through op config.

The dbt `--select` for each invocation is derived from
`context.selected_asset_keys`, so what dbt runs is always exactly what the
asset job selected — no risk of drift between the Dagster selection and an
op-config dbt selector.

Using `DbtCloudCliInvocation.run()` directly bypasses the workspace adhoc-job
constraint and lets us route each partition to a specific job in the pool.
"""

import json
from collections.abc import Sequence

import dagster as dg
from dagster_dbt.asset_utils import get_dbt_resource_names_for_asset_keys
from dagster_dbt.cloud_v2.cli_invocation import DbtCloudCliInvocation
from dagster_dbt.cloud_v2.resources import DbtCloudWorkspace, load_dbt_cloud_asset_specs
from dagster_dbt.dagster_dbt_translator import DagsterDbtTranslator


class DbtChainConfig(dg.Config):
    pool_job_ids: list[int]


def build_dbt_chain_assets(
    workspace: DbtCloudWorkspace,
    *,
    name: str,
    select: str,
    partitions_def: dg.PartitionsDefinition,
) -> dg.AssetsDefinition:
    """Build one multi_asset covering every dbt asset selected by `select`.

    `select` defines the pipeline's scope at definition time (e.g.
    `group:acxiom_demo`). At run time, the dbt `--select` is derived from the
    asset keys the executing job actually selected, so subsetting is honored
    automatically.
    """
    translator = DagsterDbtTranslator()
    specs: Sequence[dg.AssetSpec] = load_dbt_cloud_asset_specs(
        workspace=workspace,
        dagster_dbt_translator=translator,
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

        ws_data = dbt_cloud_workspace.get_or_fetch_workspace_data()

        dbt_resource_names = get_dbt_resource_names_for_asset_keys(
            translator=translator,
            manifest=ws_data.manifest,
            assets_def=context.assets_def,
            asset_keys=context.selected_asset_keys,
        )
        # dbt selector union: space-separated FQNs.
        # https://docs.getdbt.com/reference/node-selection/set-operators#unions
        dbt_select = " ".join(dbt_resource_names)

        args: list[str] = [
            "build",
            "--select",
            dbt_select,
            "--vars",
            json.dumps({"partition_id": partition_key}),
        ]

        context.log.info(
            f"Running dbt Cloud job {chosen_job_id} for partition '{partition_key}' "
            f"with args: {args}"
        )

        invocation = DbtCloudCliInvocation.run(
            job_id=chosen_job_id,
            args=args,
            client=dbt_cloud_workspace.get_client(),
            manifest=ws_data.manifest,
            dagster_dbt_translator=translator,
            context=context,
        )
        yield from invocation.wait()

    return _dbt_chain_assets
