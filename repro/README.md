# Repro: dbt_assertions + dbt Fusion — "map has no method named pop"

Minimal project to reproduce the error when using `dbt_assertions` with **dbt Fusion** and assertions defined under `config.meta.assertions` (Fusion-compat path).

## Error

```
error: dbt1501: Failed to render SQL unknown method: map has no method named pop
(in models/stage_model.sql:XX:9)
(in dbt_packages/dbt_assertions/macros/assertions.sql:69:5)
(in dbt_packages/dbt_assertions/macros/assertions.sql:90:38)
```

## Cause

Under dbt Fusion, config from YAML (e.g. `config.meta.assertions`) is an **immutable** map. The package's `default__assertions` macro calls `assertions.pop('__unique__')` and `assertions.pop('__not_null__')` on the result of `get_assertions()`, which returns that config object. Immutable maps don't support `.pop()`, so the render fails.

## How to reproduce

From this directory (`repro/`):

1. Use a profiles dir that has a valid Snowflake (or other) profile. For example from the parent repo:
   ```bash
   cd repro && DBT_PROFILES_DIR=.. dbt deps
   ```
2. Run with dbt Fusion (error occurs at render, before any DB work):
   ```bash
   DBT_PROFILES_DIR=.. dbtf run -s stage_model --vars '{"partition_id": "001"}'
   ```
   Or from repo root:
   ```bash
   dbtf run --project-dir repro -s stage_model --vars '{"partition_id": "001"}'  # uses default profiles dir
   ```

You should see the `map has no method named pop` error when the stage model is rendered.

## Fix (for issue / PR)

In `dbt_packages/dbt_assertions/macros/fusion_compat.sql`, have `get_assertions()` return a **mutable copy** of the assertions dict (e.g. build a new dict from the config) instead of the raw config object, so `.pop()` in `default__assertions` is valid. See the patch in the parent repo's `dbt_packages/.../fusion_compat.sql` for a reference implementation.
