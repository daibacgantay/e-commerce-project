WITH deduped AS (
    SELECT
        geolocation_zip_code_prefix AS zip_code,
        geolocation_lat             AS latitude,
        geolocation_lng             AS longitude,
        INITCAP(geolocation_city)   AS city,
        UPPER(geolocation_state)    AS state,
        ROW_NUMBER() OVER (
            PARTITION BY geolocation_zip_code_prefix
            ORDER BY geolocation_lat
        ) AS rn
    FROM {{ source('bronze', 'geolocation') }}
)

SELECT
    zip_code,
    latitude,
    longitude,
    city,
    state
FROM deduped
WHERE rn = 1
