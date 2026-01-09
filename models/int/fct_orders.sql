select
    order_id,
    customer_id,
    order_timestamp,
    store_id,
    subtotal,
    tax_paid,
    order_total,
    current_timestamp() as last_model_run,
    'test' as test_col
from {{ ref('stg_orders') }}