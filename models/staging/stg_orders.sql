with source as (
    select * from {{source('jaffle_shop','raw_orders')}}
),
renamed as (
    select
        ID as order_id,
        CUSTOMER as customer_id,
        ORDERED_AT as order_timestamp,
        STORE_ID as store_id,
        SUBTOTAL as subtotal,
        TAX_PAID as tax_paid,
        ORDER_TOTAL as order_total,
        current_timestamp() as last_model_run
    from source
)
select * from renamed
