SELECT
    customer_id,
    customer_unique_id,
    zip_code,
    city,
    state,
    latitude,
    longitude
FROM {{ ref('int_customers') }}
