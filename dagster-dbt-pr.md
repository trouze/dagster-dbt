# dagster-dbt: redundant `dbt parse` jobs on code-server load

Materials for filing the issue + PR against `dagster-io/dagster`.

---

## Issue

### Title

`dagster-dbt`: `DbtCloudWorkspace.fetch_workspace_data` triggers one adhoc `dbt parse` job per `@dbt_cloud_assets` decoration instead of once per code-server load

### Body

When a code location defines multiple `@dbt_cloud_assets` decorators against the same `DbtCloudWorkspace` with different `select` strings, every distinct `select` triggers its own adhoc `dbt parse` job in dbt Cloud at definition-load time. For a project with N distinct selections we observe N+ parse jobs on every `dagster dev` boot, where 1 would suffice.

The parse runs no selection (`dbt parse` with no args), so every invocation produces the same manifest — there's no semantic reason for them to be distinct.

#### Root cause

`DbtCloudWorkspace.fetch_workspace_data` is decorated `@cached_method`, which memoizes per Python instance:

`python_modules/libraries/dagster-dbt/dagster_dbt/cloud_v2/resources.py` (current `main`):

```python
@cached_method
def fetch_workspace_data(self) -> DbtCloudWorkspaceData:
    adhoc_job = self._get_or_create_dagster_adhoc_job()
    run_handler = DbtCloudJobRunHandler.run(
        job_id=adhoc_job.id,
        args=["parse"],
        client=self.get_client(),
    )
    ...
```

`load_specs` (called by `@dbt_cloud_assets` during decoration) discards the instance the cache is attached to:

```python
@cached_method
def load_specs(self, select, exclude, selector, dagster_dbt_translator=None):
    ...
    with self.process_config_and_initialize_cm() as initialized_workspace:
        defs = DbtCloudWorkspaceDefsLoader(
            workspace=initialized_workspace,  # fresh instance, empty cache
            ...
        ).build_defs()
```

`process_config_and_initialize_cm()` (in `dagster._config.pythonic_config.resource`) builds a fresh `DbtCloudWorkspace` each time via `from_resource_context_cm`. The new instance starts with an empty `@cached_method` cache, so the inner `fetch_workspace_data` call fires another parse — even though the workspace identity (`project_id` + `environment_id`) is unchanged.

The cache is bolted to the wrong identity. `unique_id` (defined as `f"{project_id}-{environment_id}"`) is the actual semantic key.

#### Expected vs actual

| Project shape                                                         | Expected parses | Actual parses |
| --------------------------------------------------------------------- | --------------- | ------------- |
| 1 `@dbt_cloud_assets` decorator                                       | 1               | 1             |
| 2 `@dbt_cloud_assets` decorators, distinct `select`                   | 1               | 2             |
| 2 decorators + explicit `workspace.get_or_fetch_workspace_data()`     | 1               | 3             |

Each extra parse is a real dbt Cloud job run — slow boot, noise in run history, wasted compute.

#### Environment

- `dagster` and `dagster-dbt` on `main` (reproduced at commit `<fill in>`)
- Python 3.10+
- Any dbt Cloud project

#### Related

Cross-process reconstruction (run workers, sensor daemon) is already handled correctly via `StateBackedDefinitionsLoader.get_or_fetch_state` + `DefinitionsLoadContext.reconstruction_metadata`. This bug is strictly the within-process case (the code server during `INITIALIZATION`).

---

## Minimal repro

A two-file dagster project. Both `@dbt_cloud_assets` decorators target the same workspace; they only differ in `select`.

`repro/definitions.py`:

```python
import os

import dagster as dg
from dagster_dbt.cloud_v2 import dbt_cloud_assets
from dagster_dbt.cloud_v2.resources import DbtCloudCredentials, DbtCloudWorkspace

creds = DbtCloudCredentials(
    account_id=os.environ["DBT_CLOUD_ACCOUNT_ID"],
    access_url=os.environ["DBT_CLOUD_ACCESS_URL"],
    token=os.environ["DBT_CLOUD_TOKEN"],
)

workspace = DbtCloudWorkspace(
    credentials=creds,
    project_id=os.environ["DBT_CLOUD_PROJECT_ID"],
    environment_id=os.environ["DBT_CLOUD_ENVIRONMENT_ID"],
)


@dbt_cloud_assets(workspace=workspace, select="tag:a")
def models_a(context, dbt_cloud_workspace):
    yield from dbt_cloud_workspace.cli(["build", "--select", "tag:a"], context=context).wait()


@dbt_cloud_assets(workspace=workspace, select="tag:b")
def models_b(context, dbt_cloud_workspace):
    yield from dbt_cloud_workspace.cli(["build", "--select", "tag:b"], context=context).wait()


defs = dg.Definitions(
    assets=[models_a, models_b],
    resources={"dbt_cloud_workspace": workspace},
)
```

`repro/pyproject.toml`:

```toml
[project]
name = "dbt-cloud-parse-repro"
version = "0.0.0"
requires-python = ">=3.10"
dependencies = ["dagster", "dagster-webserver", "dagster-dbt"]

[tool.dagster]
module_name = "repro.definitions"
```

### Steps

1. Set the five `DBT_CLOUD_*` env vars against any dbt Cloud project with at least two tagged models.
2. `uv run dagster dev` (or `dagster dev`).
3. Open the dbt Cloud UI → Deploy → Run history, filter to the `DAGSTER_ADHOC_JOB__<project>__<env>` job.

### Observed

Two `dbt parse` runs queued back-to-back at boot. Add a third `@dbt_cloud_assets` with a different `select` → three runs. Pattern is N parses for N distinct selections.

### Expected

One `dbt parse` run at boot regardless of decorator count.

### Sniff test without dbt Cloud credentials

If you want to reproduce against a mock, the smaller version is:

```python
from unittest.mock import patch
from dagster_dbt.cloud_v2.resources import DbtCloudCredentials, DbtCloudWorkspace

ws = DbtCloudWorkspace(
    credentials=DbtCloudCredentials(account_id=1, access_url="x", token="y"),
    project_id=1,
    environment_id=1,
)

with patch.object(DbtCloudWorkspace, "_get_or_create_dagster_adhoc_job") as adhoc, \
     patch("dagster_dbt.cloud_v2.resources.DbtCloudJobRunHandler.run") as run:
    ws.load_asset_specs(select="tag:a", exclude="", selector="")
    ws.load_asset_specs(select="tag:b", exclude="", selector="")
    print(f"parse jobs triggered: {run.call_count}")
# prints: parse jobs triggered: 2
```

Should print `1`.

---

## PR description

### Title

`[dagster-dbt]` cache `DbtCloudWorkspace.fetch_workspace_data` by `unique_id` to dedupe parse jobs across throwaway instances

### Body

#### Summary & Motivation

`DbtCloudWorkspace.fetch_workspace_data` was decorated `@cached_method`, which caches per Python instance. But `load_specs` (called by `@dbt_cloud_assets` at decoration time) enters `process_config_and_initialize_cm`, which yields a freshly initialized `DbtCloudWorkspace`. Each fresh instance has its own empty cache, so every `@dbt_cloud_assets` decorator with a distinct `select` triggers a new adhoc `dbt parse` job — N parse jobs for N selections on every code-server load.

The two instances differ only in Python identity; their `(project_id, environment_id)` is identical, and `dbt parse` returns the same manifest regardless. The cache was keyed on the wrong identity.

This PR replaces the per-instance cache with a process-level dict keyed by `workspace.unique_id` (already defined as `f"{project_id}-{environment_id}"`). Concurrent fetches for the same key dedupe via a per-key `threading.Lock`. A new `invalidate_workspace_data_cache` helper supports tests and force-refresh scenarios.

Cross-process behavior is unchanged: run workers / sensor daemon already short-circuit via `DefinitionsLoadContext.reconstruction_metadata` during `RECONSTRUCTION` loads. This PR only fixes the within-process case (code server during `INITIALIZATION`).

Closes #<issue number>.

#### Implementation notes

- Drops `@cached_method` from `fetch_workspace_data`.
- Adds module-level `_WORKSPACE_DATA_CACHE` keyed by `unique_id`.
- Double-checked locking: lock-free fast path, re-check inside the per-key lock to handle the race where two threads both miss and queue for the lock.
- Per-key locks created lazily under a single guard mutex around `dict.setdefault`.
- `load_specs` and the `process_config_and_initialize_cm` flow are intentionally left alone. They still re-run on each distinct `select`, but the expensive work (the parse job) is now cached; the remaining `build_dbt_specs` work runs against an in-memory manifest and is cheap.

## Test Plan

New file `python_modules/libraries/dagster-dbt/dagster_dbt_tests/cloud_v2/test_workspace_data_cache.py`:

- Two `DbtCloudWorkspace` instances with the same `unique_id` → exactly one parse triggered across both.
- Two instances with different `unique_id` → two parses (cache isolation by key).
- 8 threads racing on `fetch_workspace_data` for the same key → exactly one parse.
- `invalidate_workspace_data_cache(unique_id)` forces a re-fetch on the next call; `invalidate_workspace_data_cache()` clears all.

An autouse `pytest` fixture in `cloud_v2/conftest.py` clears the cache before and after each test so cross-test state doesn't leak.

Local smoke test on a real dbt Cloud project with two `@dbt_cloud_assets` decorators: parse-job count on code-server boot dropped from 3 → 1 (the third being an explicit `workspace.get_or_fetch_workspace_data()` warm-up call that was previously a no-op and now correctly primes the shared cache).

## Changelog

`[dagster-dbt]` `DbtCloudWorkspace.fetch_workspace_data` is now cached process-wide by `(project_id, environment_id)`. Code locations with multiple `@dbt_cloud_assets` decorators no longer trigger redundant adhoc `dbt parse` jobs on each code-server load. Added `invalidate_workspace_data_cache` for tests and force-refresh use cases.

#### Out of scope (follow-ups)

- `DbtCloudWorkspace.load_specs` is also `@cached_method` on a throwaway instance. The cost is local CPU only (manifest already cached by this PR), so leaving it for a follow-up.
- The deeper refactor — hoisting `process_config_and_initialize_cm` out of `load_specs` so the inner `@cached_method` cache lives on a stable instance — is a behavior-preserving but larger API surface change. Worth doing, not in this PR.
- `StateBackedDefinitionsLoader.get_or_fetch_state` could read from pending reconstruction metadata during `INITIALIZATION` to dedupe across any state-backed integration (Airbyte, Fivetran, Tableau loaders all have this shape). Separate PR against dagster core.
