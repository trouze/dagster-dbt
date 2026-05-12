# Custom `dbt_cloud_assets` decorator

## Why we departed from the library

`dagster_dbt.cloud_v2.dbt_cloud_assets` triggers a `dbt parse` job for every
unique `(select, exclude, selector)` combination passed to the decorator. With
many asset subsets this means N parse jobs on every code location load —
identical jobs that can't return different information because the manifest is
workspace-scoped, not selection-scoped.

The root cause is a caching architecture flaw in the library:

1. `load_specs(select, exclude, selector)` is `@cached_method` keyed on those
   args, so each unique selection is a cache miss.
2. Each miss calls `process_config_and_initialize_cm()`, which produces a fresh
   `initialized_workspace` instance.
3. `fetch_workspace_data()` is `@cached_method` on that new instance — empty
   cache — so every miss triggers a parse job.
4. The manifest (the only thing the parse produces) is buried inside the
   initialized instance and never surfaces to the caller.

`select`, `exclude`, and `selector` are post-fetch filters applied in
`build_dbt_specs`. There is no reason to re-parse the project to apply a
different filter.

## The escape hatch

`DbtCloudWorkspace.get_or_fetch_workspace_data()` is the key. Unlike
`load_specs`, it passes `workspace=self` (the original object) to
`DbtCloudWorkspaceDefsLoader`, so `fetch_workspace_data()` is cached on the
workspace instance the caller holds — shared across all decorator calls for
that workspace.

```python
workspace_data = workspace.get_or_fetch_workspace_data()
# workspace_data.manifest is now available — one parse, cached on workspace
```

With the manifest in hand, `build_dbt_specs` (the same function the library
calls internally in `defs_from_state`) applies dbt node selection locally:

```python
asset_specs, check_specs = build_dbt_specs(
    manifest=workspace_data.manifest,
    translator=translator,
    select=select,
    exclude=exclude,
    selector=selector,
    io_manager_key=None,
    project=None,
)
```

`build_dbt_specs` calls `select_unique_ids` internally, which runs full dbt
graph traversal against the manifest dict — `+`, `@`, path globs, tag/config
selectors all work. No network call, no parse job.

## How our decorator differs

Our decorator in `framework/dbt_runner.py` is a drop-in replacement. The
signature and all parameters are identical to the library's. The differences
are internal:

| | `dagster_dbt.cloud_v2.dbt_cloud_assets` | `framework/dbt_runner.dbt_cloud_assets` |
|---|---|---|
| Manifest fetch | `workspace.load_specs()` → `process_config_and_initialize_cm()` → new instance → `fetch_workspace_data()` | `workspace.get_or_fetch_workspace_data()` directly on the shared workspace instance |
| Parse jobs on load | One per unique `(select, exclude, selector)` combination | One per workspace, regardless of how many decorators share it |
| dbt node selection | `build_dbt_specs` called inside the initialized workspace context | `build_dbt_specs` called directly with the cached manifest |
| Cloud metadata | Applied in `defs_from_state` inside `StateBackedDefinitionsLoader` | Applied inline after `build_dbt_specs` returns |
| `op_tags` | Set for runtime `workspace.cli()` selection | Identical — same keys, same values |
| `can_subset` | `True` | `True` |

The `op_tags` contract is preserved exactly: `DAGSTER_DBT_SELECT_METADATA_KEY`,
`DAGSTER_DBT_EXCLUDE_METADATA_KEY`, and `DAGSTER_DBT_SELECTOR_METADATA_KEY`
are set to the decorator's `select`/`exclude`/`selector` args. At runtime,
`workspace.cli()` reads these tags (and the execution context's selected asset
keys) to build the `--select` args passed to dbt — that path is unchanged.

## What to watch on dagster-dbt upgrades

`build_dbt_specs` is not decorated `@public` in the library. It is stable and
used by the library itself, but it could change signature between minor
versions. Pin `dagster-dbt` and review `build_dbt_specs` and
`get_or_fetch_workspace_data` when upgrading.
