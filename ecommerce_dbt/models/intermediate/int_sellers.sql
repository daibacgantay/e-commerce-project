SELECT
    s.seller_id,
    s.zip_code,
    s.city,
    s.state,
    g.latitude,
    g.longitude
FROM {{ ref('staging_sellers') }} s
LEFT JOIN {{ ref('staging_geolocation') }} g ON s.zip_code = g.zip_code
