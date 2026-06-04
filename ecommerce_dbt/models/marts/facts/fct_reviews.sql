{{
    config(
        unique_key='review_id'
    )
}}

SELECT
    r.review_id,
    r.order_id,
    r.review_score,
    r.review_title,
    r.review_message,
    r.review_created_at,
    r.review_answered_at,
    DATEDIFF('day', o.delivered_at, r.review_created_at) AS days_after_delivery
FROM {{ ref('int_reviews') }} r
LEFT JOIN {{ ref('int_orders') }} o ON r.order_id = o.order_id
