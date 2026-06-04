{{
    config(
        unique_key=['order_id', 'payment_sequential']
    )
}}

SELECT
    p.order_id,
    p.payment_sequential,
    p.payment_type,
    p.payment_installments,
    p.payment_value,
    o.customer_id,
    o.current_status,
    o.purchased_at
FROM {{ ref('staging_order_payments') }} p
LEFT JOIN {{ ref('int_orders') }} o ON p.order_id = o.order_id
