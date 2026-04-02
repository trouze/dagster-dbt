import dagster as dg

from dagster_partition_demo.assets.partition_pipeline import (
    SnowflakeResource,
    name_address_refresh,
    partition_dbt_run,
    partition_hygiene_processing,
)
from dagster_partition_demo.partitions import file_partitions
from dagster_partition_demo.resources.dbt_cloud_pool import DbtCloudJobPool

partition_processing_job = dg.define_asset_job(
    name="partition_processing_job",
    selection=dg.AssetSelection.assets(partition_dbt_run),
    partitions_def=file_partitions,
    executor_def=dg.multiprocess_executor.configured({"max_concurrent": 3}),
)

hygiene_processing_job = dg.define_asset_job(
    name="hygiene_processing_job",
    selection=dg.AssetSelection.assets(partition_hygiene_processing),
    partitions_def=file_partitions,
    executor_def=dg.multiprocess_executor.configured({"max_concurrent": 3}),
)

name_address_refresh_job = dg.define_asset_job(
    name="name_address_refresh_job",
    selection=dg.AssetSelection.assets(name_address_refresh),
)

defs = dg.Definitions(
    assets=[partition_dbt_run, partition_hygiene_processing, name_address_refresh],
    resources={
        "dbt_cloud_pool": DbtCloudJobPool(
            account_id=dg.EnvVar("DBT_CLOUD_ACCOUNT_ID"),
            api_token=dg.EnvVar("DBT_CLOUD_API_TOKEN"),
            dbt_cloud_base_url=dg.EnvVar("DBT_BASE_URL"),
            project_id=dg.EnvVar.int("DBT_CLOUD_PROJECT_ID"),
        ),
        "snowflake": SnowflakeResource(
            account=dg.EnvVar("SNOWFLAKE_ACCOUNT"),
            user=dg.EnvVar("SNOWFLAKE_USER"),
            warehouse=dg.EnvVar("SNOWFLAKE_WAREHOUSE"),
            database=dg.EnvVar("SNOWFLAKE_DATABASE"),
            schema_name="TROUZE",
            private_key_path=dg.EnvVar("SNOWFLAKE_KEY_PATH"),
            private_key_passphrase=dg.EnvVar("SNOWFLAKE_PASSPHRASE"),
            role=dg.EnvVar("ROLE"),
        ),
    },
    jobs=[partition_processing_job, hygiene_processing_job, name_address_refresh_job],
)
