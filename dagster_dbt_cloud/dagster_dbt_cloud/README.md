# dagster_dbt_cloud

Dagster project that wraps dbt Cloud job runs as per-model Dagster assets via
the `IsolatedDbtCloudPipeline` component (`dagster_dbt_cloud.components`).

## Refreshing the dbt Cloud workspace cache

The component caches the dbt manifest via dagster's state-backed component
framework. Three ways to refresh:

1. **Automatic (sensor):** when an `IsolatedDbtCloudPipeline` instance has
   `github_repo` set, the component emits a polling sensor that watches the
   configured branch and triggers a refresh on every new commit. This is the
   intended day-to-day path.
2. **Manual (UI):** trigger the `refresh_<state_key>` job from the Dagster UI.
   Useful for ad-hoc refreshes (e.g. when dbt Cloud env vars change without a
   new commit).
3. **CI/CD bootstrap:** before deploying to a fresh environment, run
   `dg utils refresh-defs-state` (requires `dagster-dg-cli`) to seed state for
   every state-backed component in the project.
