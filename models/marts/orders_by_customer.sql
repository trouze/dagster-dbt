with c as (
    select * from {{ ref('dim_customers') }}
),
o as (
    select * from {{ ref('fct_orders') }}
),
final as (
    select
        c.customer_id as customer_id,
        c.name as name,
        min(o.order_timestamp) as first_order_date,
        max(o.order_timestamp) as last_order_date,
        count(o.order_timestamp) as num_orders,
        current_timestamp() as last_model_run,
        'test col' as test_col
    from c
    left join o on o.customer_id = c.customer_id
    where o.customer_id is not null
    group by c.customer_id, c.name
)
select * from final
{% if target.name == 'ci' %}
    limit 10
{% endif %}
