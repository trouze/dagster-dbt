import os

from dagster_dbt.cloud_v2.resources import (
    DbtCloudCredentials,
    DbtCloudWorkspace,
)
from dagster_dbt.cloud_v2.sensor_builder import build_dbt_cloud_polling_sensor

creds = DbtCloudCredentials(
    account_id=os.getenv("DBT_CLOUD_ACCOUNT_ID"),
    access_url=os.getenv("DBT_BASE_URL"),
    token=os.getenv("DBT_API_KEY"),
)

workspace = DbtCloudWorkspace(
    credentials=creds,
    project_id=os.getenv("DBT_CLOUD_PROJECT_ID"),
    environment_id=os.getenv("DBT_CLOUD_ENVIRONMENT_ID"),
)

dbt_cloud_polling_sensor = build_dbt_cloud_polling_sensor(workspace=workspace)
