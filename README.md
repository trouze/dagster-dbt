# dbt-slim-ci
This project aims to demo the dbt slim CI functionality, as well as give a forum for discussing the --defer and --state flags in dbt CLI that make slim CI possible.

# Source and Model Freshness
Repurposed this repository for a demo of Source and model freshness in dbt. The outline for that demo is as follows:

Out of the box functionality, to simulate for a demo:

### Source freshness snapshot
```
dbt source freshness --profiles-dir .
```

Copy sources.json to artifacts
```
cp -r ./target/* ./dev-run-artifacts
```

Perform a new load job (for Roche this is AWS Glue job), we'll use dbt seed to simulate a new load of data.

```
dbt seed --profiles-dir .
```

Run a new source freshness after a load.
```
dbt source freshness --profiles-dir .
```

Only run models that have a fresh source
```
dbt run --select source_status:fresher+ --state ./dev-run-artifacts --profiles-dir .
```

## Extending freshness logic to models
Source freshness is effectively a generic test that allows you to select downstream models that have new data when sources.json suggests data is fresher. We can extend this process to models if we wish in much the same way using a custom generic test for model freshness, and run_results.json artifact. The process is as follows:

Create generic model freshness test:
```sql
{% test is_model_fresh(model, last_model_run_field, error_after, time_part) %}
{{ config(store_failures = false, severity = 'error') }}
    select
    count(*)
    from {{ model }}
    having cast(max({{ last_model_run_field }}) as TIMESTAMP) < DATEADD({{ time_part }}, -cast({{ error_after }} as integer), current_timestamp())
{% endtest %}
```

This test takes a few custom arguments, and assumes that your models have a `current_timestamp()` column that updates each time a model is ran.

We can add this test to any model we create, using yaml (or other configs in dbt).
```yaml
models:
  - name: my_model
    description: ""
    tests:
      - is_model_fresh:
          last_model_run_field: last_model_run
          error_after: 1
          time_part: minute
```

This test will fail if our model hasn't ran in over a minute, for example. `time_part` argument maps to time_part in most SQL dialects, Snowflake's options are [here](https://docs.snowflake.com/en/sql-reference/functions-date-time.html#label-supported-date-time-parts).

Now, we can run our tests on models to check for freshness.
```
dbt test -s test_name:is_model_fresh --profiles-dir .
```

Then we'll copy the run_results.json of our tests to our artifacts directory for future reference.

```
cp ./target/run_results.json ./dev-run-artifacts/run_results.json
```

Then we can run only models that are considered not fresh.
```
dbt run --select 1+result:fail+ --state ./dev-run-artifacts --profiles-dir .
```

This will select models that had a failing tests. This helps us keep ourselves from running fresh models over again, since tests can be much more computationally light.

# Slim CI in dbt
commands:
#### Setup
```
dbt clean
dbt deps
```
#### Production run and manifest.json creation
```
dbt run --profiles-dir .
#### Copy manifest.json from you production run
mkdir dev-run-artifacts && cp ./target/manifest.json ./dev-run-artifacts/manifest.json
```
#### Change a model
orders_by_customers.sql changes

#### Slim CI Run
```
#### State can also be an environment variable
dbt run --select state:modified --profiles-dir . --target ci --defer --state ./dev-run-artifacts
#### Drop temporary CI_ Schemas
dbt run-operation remove_mr_schemas --profiles-dir .
```

#### Slim CI
```
dbt run -m state:modified+1 1+exposure:*,state:modified+ --profiles-dir . --target ci --defer --state ./dev-run-artifacts
```
- Modified model and first order children
- Any exposure that had an upstream model changed


## Resources
- [Deferral](https://docs.getdbt.com/reference/node-selection/defer)
- [Understanding State](https://docs.getdbt.com/guides/legacy/understanding-state)
