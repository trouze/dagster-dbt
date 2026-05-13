"""One-shot lookup of the dbt Cloud CI job ID.

CI jobs allow concurrent runs of the same job (unlike deploy jobs, which queue),
so a single CI job ID — resolved once at code-location load time — backs every
DbtCloudCliInvocation in the pipeline.
"""

from dagster_dbt.cloud_v2.resources import DbtCloudWorkspace


def fetch_job_id(workspace: DbtCloudWorkspace) -> int:
    """Return the dbt Cloud CI job ID for the workspace's project/environment.

    Raises if zero or multiple CI jobs are found — the caller should be explicit
    rather than have one silently picked.
    """
    jobs = workspace.get_client().list_jobs(
        project_id=int(workspace.project_id),
        environment_id=int(workspace.environment_id),
    )
    ci_jobs = [job for job in jobs if job.get("job_type") == "ci"]
    if not ci_jobs:
        raise ValueError(
            f"No dbt Cloud CI job found in project {workspace.project_id} / "
            f"environment {workspace.environment_id}."
        )
    if len(ci_jobs) > 1:
        names = [j.get("name") for j in ci_jobs]
        raise ValueError(
            f"Expected exactly one CI job in project {workspace.project_id} / "
            f"environment {workspace.environment_id}; found {len(ci_jobs)}: {names}."
        )
    return int(ci_jobs[0]["id"])
