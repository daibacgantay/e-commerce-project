SELECT
    order_id,
    order_item_id,
    product_id,
    seller_id,
    TO_TIMESTAMP_NTZ(shipping_limit_date::NUMBER, 6) AS shipping_limit_at,
    price::FLOAT                                     AS price,
    freight_value::FLOAT                             AS freight_value,
    _op,
    _ingested_at::TIMESTAMP         AS ingested_at
FROM {{ source('bronze', 'order_items') }}
WHERE _op != 'd'
