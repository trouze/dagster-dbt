
with source as (
    select *
    from {{ source('jaffle_shop','raw_customers') }}
),
renamed as (
    select
        ID as customer_id,
        NAME as name,
        'test' as test_col,
        SIGNUP_DATE as signup_date,
        LOAD_DTS as load_dts,
        current_timestamp() as last_model_run
    from source
)
select * from renamed
