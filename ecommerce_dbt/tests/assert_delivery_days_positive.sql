SELECT order_id
FROM {{ ref('fct_orders') }}
WHERE delivery_days < 0
