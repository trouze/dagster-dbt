import dagster as dg
from dagster_dbt.cloud_v2.resources import DbtCloudCredentials, DbtCloudWorkspace

creds = DbtCloudCredentials(
    account_id=dg.EnvVar.int("DBT_CLOUD_ACCOUNT_ID").get_value(),
    access_url=dg.EnvVar("DBT_BASE_URL").get_value(),
    token=dg.EnvVar("DBT_API_KEY").get_value(),
)

dbt_cloud_workspace = DbtCloudWorkspace(
    credentials=creds,
    project_id=dg.EnvVar.int("DBT_CLOUD_PROJECT_ID").get_value(),
    environment_id=dg.EnvVar.int("DBT_CLOUD_ENVIRONMENT_ID").get_value(),
)
