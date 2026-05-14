"""GitHub PAT-backed client for branch-head polling.

Used by the dbt-project-change sensor emitted by `IsolatedDbtCloudPipeline`.
Read-only: only fetches the latest commit SHA for a configured branch so the
sensor can decide whether to trigger a parse-job refresh.
"""

import os

import dagster as dg
import requests
from pydantic import Field


class GitHubResource(dg.ConfigurableResource):
    """Minimal GitHub REST client for `GET /repos/{repo}/branches/{branch}`.

    Token must have `contents: read` on each polled repo (fine-grained PAT) or
    `repo` scope (classic PAT). 30-second request timeout to keep sensor ticks
    bounded.
    """

    token: str = Field(description="GitHub PAT.")
    api_url: str = Field(
        default="https://api.github.com",
        description="GitHub API base URL. Override for GitHub Enterprise.",
    )

    def get_branch_head_sha(self, repo: str, branch: str) -> str:
        response = requests.get(
            f"{self.api_url}/repos/{repo}/branches/{branch}",
            headers={
                "Authorization": f"Bearer {self.token}",
                "Accept": "application/vnd.github+json",
                "X-GitHub-Api-Version": "2022-11-28",
            },
            timeout=30,
        )
        response.raise_for_status()
        return response.json()["commit"]["sha"]


def build_github_resource() -> GitHubResource:
    return GitHubResource(token=os.environ["GITHUB_TOKEN"])
