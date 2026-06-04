SELECT
    seller_id,
    zip_code,
    city,
    state,
    latitude,
    longitude
FROM {{ ref('int_sellers') }}
