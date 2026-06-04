{{
    config(
        unique_key='order_id'
    )
}}

SELECT
    o.order_id,
    o.customer_id,
    o.current_status,
    o.purchased_at,
    o.approved_at,
    o.shipped_at,
    o.delivered_at,
    o.estimated_delivery_at,
    o.delivery_days,
    o.total_items,
    o.total_price,
    o.total_freight,
    o.total_amount,
    d.date_id AS purchase_date_id
FROM {{ ref('int_orders') }} o
LEFT JOIN {{ ref('dim_dates') }} d ON o.purchased_at::DATE = d.date_id
