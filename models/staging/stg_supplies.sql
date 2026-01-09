with source as (
    select *
    from {{ source('jaffle_shop','raw_supplies') }}
),
renamed as (
    select
        ID as supply_id,
        NAME as name,
        COST as cost,
        SKU as product_id,
        LOAD_DTS as load_dts,
        current_timestamp() as last_model_run
    from source
)
select * from renamed