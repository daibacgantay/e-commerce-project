WITH order_totals AS (
    SELECT
        order_id,
        COUNT(order_item_id)        AS total_items,
        SUM(price)                  AS total_price,
        SUM(freight_value)          AS total_freight,
        SUM(price + freight_value)  AS total_amount
    FROM {{ ref('staging_order_items') }}
    GROUP BY order_id
)

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
    t.total_items,
    t.total_price,
    t.total_freight,
    t.total_amount
FROM {{ ref('int_orders_pivoted') }} o
LEFT JOIN order_totals t ON o.order_id = t.order_id
