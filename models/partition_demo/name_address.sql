{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='customer_id',
        static_analysis="baseline"
    )
}}

with refined_addresses as (

    select distinct
        customer_id,
        store_id
    from {{ ref('file_orders_refined') }}

),

hygiene as (

    select
        customer_id,
        hygiene_status,
        corrected_name,
        corrected_address,
        last_hygiene_date,
        inserted_at
    from {{ ref('hygiene_results') }}
    {% if is_incremental() %}
    where inserted_at > coalesce(
        (select max(inserted_at) from {{ this }}),
        '1900-01-01 00:00:00'::timestamp_ntz
    )
    {% endif %}
    qualify row_number() over (
        partition by customer_id
        order by inserted_at desc
    ) = 1

),

final as (

    select
        r.customer_id,
        r.store_id,
        h.hygiene_status,
        h.corrected_name,
        h.corrected_address,
        h.last_hygiene_date,
        h.inserted_at
    from refined_addresses r
    inner join hygiene h
        on r.customer_id = h.customer_id
)

select * from final
