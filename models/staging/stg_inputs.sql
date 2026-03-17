-- stg_inputs.sql
{{ config(materialized='view') }}

select *
from {{ ref('customer_history') }}
where dbt_valid_to is null
  and dbt_valid_from > '{{ var("last_run_timestamp", "1900-01-01") }}'