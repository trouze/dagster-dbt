"""Builder for a shared `DbtCloudWorkspace` resource.

Used by the Python `hygiene_mock` asset to look up manifest-derived database/
schema locations. Components configured via YAML build their own workspace from
the same env vars; the state-backed framework dedupes parse jobs by
`workspace.unique_id`, so this resource hits the same cache.
"""

import os

from dagster_dbt.cloud_v2.resources import DbtCloudCredentials, DbtCloudWorkspace


def build_workspace() -> DbtCloudWorkspace:
    return DbtCloudWorkspace(
        credentials=DbtCloudCredentials(
            account_id=os.environ["DBT_CLOUD_ACCOUNT_ID"],
            access_url=os.environ["DBT_BASE_URL"],
            token=os.environ["DBT_API_KEY"],
        ),
        project_id=os.environ["DBT_CLOUD_PROJECT_ID"],
        environment_id=os.environ["DBT_CLOUD_ENVIRONMENT_ID"],
    )
