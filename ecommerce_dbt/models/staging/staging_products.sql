SELECT
    product_id,
    NULLIF(TRIM(product_category_name), '') AS product_category,
    product_name_length::INT                AS product_name_length,
    product_description_length::INT         AS product_description_length,
    product_photos_qty::INT                 AS product_photos_qty,
    product_weight_g::FLOAT                 AS weight_g,
    product_length_cm::FLOAT                AS length_cm,
    product_height_cm::FLOAT                AS height_cm,
    product_width_cm::FLOAT                 AS width_cm
FROM {{ source('bronze', 'products') }}
