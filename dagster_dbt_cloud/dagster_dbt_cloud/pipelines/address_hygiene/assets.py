"""Assets for the address-hygiene pipeline.

- One AssetsDefinition per dbt model in the `acxiom_demo` group, each its
  own op (independent retry boundary, one dbt Cloud run per model).
- `hygiene_mock`: bridge between chain1 and chain2 — calls the dbt-managed
  `address_hygiene_pending` function, runs results through a fake hygiene
  API, and inserts into `hygiene_results`.
"""

from collections.abc import Sequence

import dagster as dg
from dagster_dbt.cloud_v2.resources import DbtCloudWorkspace

from dagster_dbt_cloud.framework.dbt_runner import build_dbt_chain_assets
from dagster_dbt_cloud.framework.snowflake import (
    SnowflakeResource,
    dedupe_pending_rows,
    simulate_hygiene_api,
)
from dagster_dbt_cloud.framework.sources import (
    find_function_location_in_manifest,
    find_model_location_in_manifest,
)

from .partitions import file_partitions

# Asset key for the dbt model that bridges chain1 and chain2. The dbt
# translator uses the configured schema as the asset-key prefix, so
# `name_address` lives at `["partition_demo", "name_address"]`.
NAME_ADDRESS_KEY = dg.AssetKey(["partition_demo", "name_address"])

# chain1 builds every dbt model in the acxiom_demo group. We select on group
# rather than `name_address.upstream()` because rejects (a sibling of refined
# off file_orders_stage) is not upstream of name_address and would otherwise
# be skipped.
chain1_selection = dg.AssetSelection.groups("acxiom_demo")
# chain2 re-runs name_address incrementally after hygiene_mock has populated
# hygiene_results.
chain2_selection = dg.AssetSelection.keys(NAME_ADDRESS_KEY)


def build_dbt_assets(
    workspace: DbtCloudWorkspace,
    pool_job_ids: list[int],
) -> Sequence[dg.AssetsDefinition]:
    return build_dbt_chain_assets(
        workspace,
        group_name="acxiom_demo",
        partitions_def=file_partitions,
        pool_job_ids=pool_job_ids,
    )


@dg.asset(
    name="hygiene_mock",
    partitions_def=file_partitions,
    deps=[NAME_ADDRESS_KEY],
    description=(
        "External hygiene API mock. For a partition, calls the "
        "address_hygiene_pending function, runs results through the fake "
        "hygiene API, and inserts into hygiene_results."
    ),
)
def hygiene_mock(
    context: dg.AssetExecutionContext,
    dbt_cloud_workspace: DbtCloudWorkspace,
    snowflake: SnowflakeResource,
) -> dg.MaterializeResult:
    partition_id = context.partition_key

    # Functions are dbt resources but not Dagster assets, so the chain1
    # selection skips them. Build the function on-demand before calling it;
    # the function definition isn't partition-dependent, so this is
    # idempotent (and cheap).
    context.log.info("Building dbt function: address_hygiene_pending")
    invocation = dbt_cloud_workspace.cli(
        args=["build", "--select", "address_hygiene_pending"],
    )
    list(invocation.wait())

    # Pull the location from the *just-completed* run's manifest. The
    # workspace's cached manifest can lag behind project edits (schema
    # config changes, new functions) and would resolve to a stale
    # database/schema.
    fresh_manifest = invocation.run_handler.get_manifest()
    fn_entry = next(
        (
            n
            for n in fresh_manifest.get("functions", {}).values()
            if n.get("name") == "address_hygiene_pending"
        ),
        None,
    )
    context.log.info(
        f"address_hygiene_pending manifest entry: "
        f"database={fn_entry and fn_entry.get('database')}, "
        f"schema={fn_entry and fn_entry.get('schema')}, "
        f"relation_name={fn_entry and fn_entry.get('relation_name')}, "
        f"config.schema={fn_entry and fn_entry.get('config', {}).get('schema')}"
    )
    db, schema = find_function_location_in_manifest(
        fresh_manifest, "address_hygiene_pending"
    )
    sql = f"SELECT * FROM TABLE({db}.{schema}.address_hygiene_pending(%s))"
    pending = snowflake.execute(sql, (partition_id,))
    context.log.info(
        f"address_hygiene_pending returned {len(pending)} rows for partition '{partition_id}'."
    )

    deduped = dedupe_pending_rows(pending)
    results = simulate_hygiene_api(deduped)

    hr_db, hr_schema, hr_name = find_model_location_in_manifest(
        fresh_manifest, "hygiene_results"
    )
    target_relation = f'"{hr_db}"."{hr_schema}"."{hr_name}"'
    snowflake.insert_hygiene_results(results, target_relation=target_relation)

    return dg.MaterializeResult(
        metadata={
            "partition_id": partition_id,
            "pending_row_count": len(pending),
            "deduped_row_count": len(deduped),
            "inserted_row_count": len(results),
        }
    )
