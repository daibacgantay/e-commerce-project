{{
    config(
        unique_key=['order_id', 'order_item_id']
    )
}}

SELECT
    oi.order_id,
    oi.order_item_id,
    oi.product_id,
    oi.seller_id,
    oi.price,
    oi.freight_value,
    oi.total_item_amount,
    oi.shipping_limit_at,
    oi.product_category,
    oi.weight_g,
    oi.seller_city,
    oi.seller_state
FROM {{ ref('int_order_items') }} oi
