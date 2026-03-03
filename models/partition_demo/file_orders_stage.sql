{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        unique_key='file_id',
        event_time='ordered_at'
    )
}}
{% set source_alias = 'inbound_file_orders_' + var('partition_id') %}

with source as (

    select
        order_id,
        customer_id,
        ordered_at,
        store_id,
        subtotal,
        tax_paid,
        order_total,
        'test' as test_col,
        file_id
    from {{ source('partition_demo', source_alias) }}
    {% if is_incremental() %}
    where file_id = '{{ var("partition_id") }}'
    {% endif %}

),

final as (

    select
        *,
        {{ dbt_assertions.assertions() }}
    from source

)

select * from final
