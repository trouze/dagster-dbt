with source as (
    select *
    from {{ source('jaffle_shop','raw_stores') }}
),
renamed as (
    select
        ID as store_id,
        NAME as name,
        OPENED_AT as opened_on,
        TAX_RATE as tax_rate,
        LOAD_DTS as load_dts,
        current_timestamp() as last_model_run
    from source
)
select * from renamed