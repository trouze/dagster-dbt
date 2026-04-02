{{
    config(
        materialized='view',
        unique_key='id'
    )
}}

with source as (
    select
        id,
        name,
        amount
    from {{ source('repro', 'inbound_file_orders_001') }}
),

final as (
    select
        *,
        {{ dbt_assertions.assertions() }}
    from source
)

select * from final
