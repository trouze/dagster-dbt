import json

import dagster as dg
from dagster_dbt.cloud_v2 import DbtCloudWorkspace, dbt_cloud_assets

from dagster_dbt_cloud.partitions import file_partitions
from dagster_dbt_cloud.workspace import dbt_cloud_workspace


@dbt_cloud_assets(
    workspace=dbt_cloud_workspace,
    select="tag:chain1",
    partitions_def=file_partitions,
    name="file_orders_chain",
    group_name="acxiom_demo",
)
def file_orders_chain(
    context: dg.AssetExecutionContext, dbt_cloud_workspace: DbtCloudWorkspace
):
    args = ["build", "--vars", json.dumps({"partition_id": context.partition_key})]
    yield from dbt_cloud_workspace.cli(args, context=context).wait().stream()


@dbt_cloud_assets(
    workspace=dbt_cloud_workspace,
    select="name_address",
    partitions_def=file_partitions,
    name="name_address_chain",
    group_name="acxiom_demo",
)
def name_address_chain(
    context: dg.AssetExecutionContext, dbt_cloud_workspace: DbtCloudWorkspace
):
    args = ["build", "--vars", json.dumps({"partition_id": context.partition_key})]
    yield from dbt_cloud_workspace.cli(args, context=context).wait().stream()
