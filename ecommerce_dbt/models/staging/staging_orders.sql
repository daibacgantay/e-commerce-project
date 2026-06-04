    SELECT
        order_id,
        customer_id,
        order_status AS status,
        TO_TIMESTAMP_NTZ(order_purchase_timestamp::NUMBER, 6)    AS purchased_at,
        TO_TIMESTAMP_NTZ(order_approved_at::NUMBER, 6)           AS approved_at,
        TO_TIMESTAMP_NTZ(order_delivered_carrier_date::NUMBER, 6) AS shipped_at,
        TO_TIMESTAMP_NTZ(order_delivered_customer_date::NUMBER, 6) AS delivered_at,
        TO_TIMESTAMP_NTZ(order_estimated_delivery_date::NUMBER, 6) AS estimated_delivery_at,
        _op,
        _ingested_at::TIMESTAMP AS ingested_at
    FROM {{source('bronze', 'orders')}}
    WHERE _op != 'd'
