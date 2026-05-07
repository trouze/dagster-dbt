import os

import dagster as dg

from dagster_dbt_cloud.assets import (
    address_hygiene_external,
    file_intake,
    file_orders_chain,
    name_address_chain,
)
from dagster_dbt_cloud.partitions import file_partitions
from dagster_dbt_cloud.resources.snowflake import SnowflakeResource
from dagster_dbt_cloud.sensors import file_partition_sensor
from dagster_dbt_cloud.sensors.file_sensor import DEMO_PIPELINE_JOB_NAME
from dagster_dbt_cloud.workspace import dbt_cloud_workspace

demo_pipeline_job = dg.define_asset_job(
    name=DEMO_PIPELINE_JOB_NAME,
    selection=dg.AssetSelection.assets(
        file_intake,
        file_orders_chain,
        address_hygiene_external,
        name_address_chain,
    ),
    partitions_def=file_partitions,
)

defs = dg.Definitions(
    assets=[
        file_intake,
        file_orders_chain,
        address_hygiene_external,
        name_address_chain,
    ],
    resources={
        "dbt_cloud_workspace": dbt_cloud_workspace,
        "snowflake": SnowflakeResource(
            account=dg.EnvVar("SNOWFLAKE_ACCOUNT"),
            user=dg.EnvVar("SNOWFLAKE_USER"),
            warehouse=dg.EnvVar("SNOWFLAKE_WAREHOUSE"),
            database=dg.EnvVar("SNOWFLAKE_DATABASE"),
            schema_name=os.getenv("SNOWFLAKE_SCHEMA", "TROUZE"),
            hygiene_results_database=os.getenv("HYGIENE_RESULTS_DATABASE"),
            hygiene_results_schema=os.getenv("HYGIENE_RESULTS_SCHEMA"),
            hygiene_results_table=os.getenv("HYGIENE_RESULTS_TABLE", "hygiene_results"),
            private_key_path=dg.EnvVar("SNOWFLAKE_KEY_PATH"),
            private_key_passphrase=dg.EnvVar("SNOWFLAKE_PASSPHRASE"),
            role=dg.EnvVar("ROLE"),
        ),
    },
    jobs=[demo_pipeline_job],
    sensors=[file_partition_sensor],
)
