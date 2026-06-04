SELECT
    c.customer_id,
    c.customer_unique_id,
    c.zip_code,
    c.city,
    c.state,
    g.latitude,
    g.longitude
FROM {{ ref('staging_customers') }} c
LEFT JOIN {{ ref('staging_geolocation') }} g ON c.zip_code = g.zip_code
