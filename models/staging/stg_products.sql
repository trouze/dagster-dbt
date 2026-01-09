with source as (
    select *
    from {{ source('jaffle_shop','raw_products') }}
),
renamed as (
    select
        SKU as product_id,
        NAME as name,
        TYPE as type,
        PRICE as price,
        DESCRIPTION as description,
        load_dts as load_dts,
        current_timestamp() as last_model_run
    from source
)
select * from renamed