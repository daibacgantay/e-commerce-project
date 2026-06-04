SELECT
    oi.order_id,
    oi.order_item_id,
    oi.product_id,
    oi.seller_id,
    oi.price,
    oi.freight_value,
    oi.price + oi.freight_value AS total_item_amount,
    oi.shipping_limit_at,
    p.product_category,
    p.weight_g,
    s.city      AS seller_city,
    s.state     AS seller_state
FROM {{ ref('staging_order_items') }} oi
LEFT JOIN {{ ref('staging_products') }} p ON oi.product_id = p.product_id
LEFT JOIN {{ ref('staging_sellers') }}  s ON oi.seller_id  = s.seller_id
