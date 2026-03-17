{{
    config(
        materialized='incremental',
        incremental_strategy='append'
    )
}}

select
    null::varchar       as customer_id,
    null::varchar       as hygiene_status,
    null::varchar       as corrected_name,
    null::varchar       as corrected_address,
    null::date          as last_hygiene_date,
    null::timestamp_ntz as inserted_at
where false
