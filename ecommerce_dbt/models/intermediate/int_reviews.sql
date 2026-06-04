SELECT
    review_id,
    order_id,
    review_score,
    review_title,
    review_message,
    review_created_at,
    review_answered_at,
    _op,
    ingested_at
FROM {{ ref('staging_order_reviews') }}
QUALIFY ROW_NUMBER() OVER (PARTITION BY review_id ORDER BY ingested_at DESC) = 1
