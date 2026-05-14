"""Mock preflight gate for the partition_demo pipeline.

Wired into chain1's `IsolatedDbtCloudPipeline` via the `preflight:` YAML field.
Raises to abort the asset run before dbt Cloud is invoked. Customers implement
real checks here (row counts, schema drift, freshness windows, etc.) — this
mock just logs and lets the run proceed.
"""

import dagster as dg


def run_preflight(context: dg.AssetExecutionContext) -> None:
    model = context.asset_key.path[-1]
    partition = context.partition_key if context.has_partition_key else None
    context.log.info(
        f"[preflight] OK — model={model} partition={partition}. "
        "Replace this stub with real go/no-go checks."
    )

    # --- Pattern A: fail the run (idiomatic "kill it") -----------------------
    # Raises through the asset op; dbt Cloud is never invoked. The description
    # and metadata land on the failed run in the Dagster UI.
    #
    # if not source_table_has_rows(partition):
    #     raise dg.Failure(
    #         description=f"Source table is empty for partition {partition}",
    #         metadata={
    #             "partition": partition,
    #             "rule": "non_empty_source",
    #         },
    #     )

    # --- Pattern B: soft-skip with a no-op materialization -------------------
    # No first-class "skip" inside a multi_asset op — closest equivalent is to
    # record a MaterializeResult marked as skipped and return before triggering
    # dbt. The asset shows green with skip metadata. Caveat: downstream assets
    # treat this as a fresh materialization, so only use when "skip" really
    # means "leave the existing table as-is and let downstreams proceed."
    #
    # Requires returning from `run_preflight` AND short-circuiting the caller —
    # the current component contract is "raise to abort," so to wire this in
    # you'd extend the component to honor a sentinel return value, or move the
    # skip-yield into the asset body itself.
    #
    # if upstream_is_stale(partition):
    #     yield dg.MaterializeResult(
    #         metadata={"skipped": True, "reason": "upstream stale"},
    #     )
    #     return
