WITH latest AS (
    SELECT
        order_id,
        customer_id,
        status,
        purchased_at,
        approved_at,
        shipped_at,
        delivered_at,
        estimated_delivery_at,
        ingested_at,
        ROW_NUMBER() OVER (
            PARTITION BY order_id
            ORDER BY ingested_at DESC
        ) AS rn
    FROM {{ ref('staging_orders') }}
),

pivoted AS (
    SELECT
        order_id,
        MAX(purchased_at)          AS purchased_at,
        MAX(approved_at)           AS approved_at,
        MAX(shipped_at)            AS shipped_at,
        MAX(delivered_at)          AS delivered_at,
        MAX(estimated_delivery_at) AS estimated_delivery_at
    FROM latest
    GROUP BY order_id
)


SELECT
    p.order_id,
    l.customer_id,
    l.status        AS current_status,
    p.purchased_at,
    p.approved_at,
    p.shipped_at,
    p.delivered_at,
    p.estimated_delivery_at,
    DATEDIFF('day', p.purchased_at, p.delivered_at) AS delivery_days
FROM pivoted p
JOIN latest l ON p.order_id = l.order_id AND l.rn = 1



