# dagster-dbt

A starter project for orchestrating dbt platform job runs from Dagster.

Each dbt model becomes a per-model Dagster asset, materialized through a single
dbt platform job. The bundled `IsolatedDbtCloudPipeline` component handles the
mapping, caches the dbt manifest as state-backed Dagster defs, and refreshes
that cache automatically when the watched branch advances.

## What you get

- Per-model assets in Dagster, materialized via a single dbt platform job
- Partition-aware reprocessing — pass a `partition_id` and dbt rebuilds only
  that slice
- Python steps between dbt stages (preflight checks, mocked external API calls,
  etc.) demonstrated in `defs/partition_demo/`
- Auto-refresh of the dbt manifest on every new commit via a polling git
  sensor
- Multi-chain pipelines that share one manifest cache across stages

## Repo layout

```
dagster_dbt_cloud/          # Dagster project (uv-managed)
  dagster_dbt_cloud/
    components/             # IsolatedDbtCloudPipeline component
    defs/partition_demo/    # demo: components + Python steps + preflight
    framework/              # shared sources helpers
    resources/              # github, snowflake, workspace resources

models/partition_demo/      # dbt models the demo runs against
seeds/                      # sample seed data
profiles.yml                # dbt profile (uses env vars)
dbt_project.yml             # dbt project config
```

A deeper walkthrough of the component itself lives at
[`dagster_dbt_cloud/dagster_dbt_cloud/README.md`](dagster_dbt_cloud/dagster_dbt_cloud/README.md).

## Prerequisites

- Python 3.10+
- [uv](https://docs.astral.sh/uv/)
- A dbt platform account
- A Snowflake warehouse the dbt job can write to (key-pair auth in the default
  profile)

## Quickstart

1. Clone and install:

   ```bash
   git clone https://github.com/trouze/dagster-dbt.git
   cd dagster-dbt/dagster_dbt_cloud
   uv sync
   ```

2. Point `dbt_project.yml` at your dbt platform project — replace the
   placeholder `project-id` and `tenant_hostname`.

3. Set environment variables. The component reads:

   | Variable | Purpose |
   | --- | --- |
   | `DBT_CLOUD_ACCOUNT_ID` | dbt platform account |
   | `DBT_CLOUD_PROJECT_ID` | dbt platform project |
   | `DBT_CLOUD_ENVIRONMENT_ID` | dbt platform environment |
   | `DBT_API_KEY` | API token |
   | `DBT_BASE_URL` | e.g. `https://cloud.getdbt.com` |
   | `DBT_CLOUD_GIT_BRANCH` | branch the refresh sensor watches |

   The component auto-provisions the dbt platform job on first run — no job ID
   needed. Snowflake credentials (`SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`,
   `SNOWFLAKE_DATABASE`, `SNOWFLAKE_WAREHOUSE`, `SNOWFLAKE_SCHEMA`,
   `SNOWFLAKE_KEY_PATH`, `SNOWFLAKE_PASSPHRASE`) are shared between the Dagster
   resource and `profiles.yml`.

4. Bootstrap the manifest cache:

   ```bash
   uv run dg utils refresh-defs-state
   ```

5. Run Dagster locally:

   ```bash
   uv run dg dev
   ```

## Adapting for your own pipelines

- Drop new `defs.yaml` files under `dagster_dbt_cloud/defs/<your_pipeline>/`
  with one or more `IsolatedDbtCloudPipeline` instances. The top-level
  `definitions.py` auto-loads everything under `defs/`.
- Only one component per dbt workspace should own the refresh — set
  `github_repo` on that one chain. Downstream chains share the cached manifest
  via the framework's defs-state key.
- Custom Python steps live as plain `.py` files in the same folder and are
  loaded by `dg.load_defs`.

## License

MIT — see [LICENSE](LICENSE).
