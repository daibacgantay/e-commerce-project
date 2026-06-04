SELECT
    order_id,
    payment_sequential::INT         AS payment_sequential,
    payment_type,
    payment_installments::INT       AS payment_installments,
    payment_value::FLOAT            AS payment_value,
    _op,
    _ingested_at::TIMESTAMP         AS ingested_at
FROM {{ source('bronze', 'order_payments') }}
WHERE _op != 'd'
