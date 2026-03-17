SELECT DISTINCT
    r.customer_id,
    r.order_id,
    r.store_id,
    r.file_id
FROM {{ ref('file_orders_refined') }} r
LEFT JOIN {{ ref('name_address') }} na
    ON r.customer_id = na.customer_id
WHERE (p_file_id IS NULL OR r.file_id = p_file_id)
  AND {{ function('needs_hygiene') }}(na.last_hygiene_date, p_staleness_months)
