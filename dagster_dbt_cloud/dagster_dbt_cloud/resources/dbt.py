import os
from collections.abc import Mapping, Sequence
from typing import Any

from dagster_dbt.cloud_v2.client import (
    DAGSTER_ADHOC_TRIGGER_CAUSE,
    DbtCloudWorkspaceClient,
)
from dagster_dbt.cloud_v2.resources import (
    DbtCloudCredentials,
    DbtCloudWorkspace,
)
from dagster_dbt.cloud_v2.sensor_builder import build_dbt_cloud_polling_sensor

from dagster_dbt_cloud.framework.job_lookup import fetch_job_id

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

DBT_CLOUD_GIT_BRANCH = os.getenv("DBT_CLOUD_GIT_BRANCH", "demo/custom-decorator-demo")

JOB_ID: int = fetch_job_id(workspace)


# Upstream DbtCloudWorkspaceClient.trigger_job_run does not forward git_branch.
# CI jobs need it to know which branch to check out. Patch the method to always
# include the configured branch in the trigger body. Drop this once upstream
# accepts a git_branch kwarg.
def _trigger_job_run_with_branch(
    self: DbtCloudWorkspaceClient,
    job_id: int,
    steps_override: Sequence[str] | None = None,
) -> Mapping[str, Any]:
    data: dict[str, Any] = {
        "cause": DAGSTER_ADHOC_TRIGGER_CAUSE,
        "git_branch": DBT_CLOUD_GIT_BRANCH,
    }
    if steps_override:
        data["steps_override"] = steps_override
    return self._make_request(
        method="post",
        endpoint=f"jobs/{job_id}/run",
        base_url=self.api_v2_url,
        data=data,
    ).json()["data"]


DbtCloudWorkspaceClient.trigger_job_run = _trigger_job_run_with_branch


dbt_cloud_polling_sensor = build_dbt_cloud_polling_sensor(workspace=workspace)
