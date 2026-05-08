"""One-shot lookup of dbt Cloud job IDs by name prefix.

Pipelines that fan partitions across a pool of identically-named dbt Cloud
jobs (e.g. `partition_runner_NN`) resolve those IDs once at code-location
load time by listing jobs in the configured project/environment.
"""

from dagster_dbt.cloud_v2.resources import DbtCloudWorkspace


def fetch_pool_job_ids(
    workspace: DbtCloudWorkspace,
    name_prefix: str,
) -> list[int]:
    """Return the dbt Cloud job IDs whose names start with `name_prefix`.

    Scoped to the workspace's project and environment.
    """
    jobs = workspace.get_client().list_jobs(
        project_id=workspace.project_id,
        environment_id=workspace.environment_id,
    )
    job_ids = [
        int(job["id"])
        for job in jobs
        if str(job.get("name", "")).startswith(name_prefix)
    ]
    if not job_ids:
        raise ValueError(
            f"No dbt Cloud jobs found matching prefix '{name_prefix}' "
            f"in project {workspace.project_id} / environment {workspace.environment_id}."
        )
    return sorted(job_ids)
