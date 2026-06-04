SELECT
    review_id,
    order_id,
    review_score::INT                       AS review_score,
    NULLIF(TRIM(review_comment_title), '')   AS review_title,
    NULLIF(TRIM(review_comment_message), '') AS review_message,
    TO_TIMESTAMP_NTZ(review_creation_date::NUMBER, 6)    AS review_created_at,
    TO_TIMESTAMP_NTZ(review_answer_timestamp::NUMBER, 6) AS review_answered_at,
    _op,
    _ingested_at::TIMESTAMP                 AS ingested_at
FROM {{ source('bronze', 'order_reviews') }}
WHERE _op != 'd'
