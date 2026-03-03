{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        unique_key='file_id'
    )
}}

select
    order_id,
    customer_id,
    ordered_at,
    store_id,
    subtotal,
    tax_paid,
    order_total,
    file_id,
    exceptions
from {{ ref('file_orders_stage') }}
where {{ dbt_assertions.assertions_filter(reverse=true) }}
{% if is_incremental() %}
and file_id = '{{ var("partition_id") }}'
{% endif %}
