SELECT
    product_id,
    product_category,
    product_name_length,
    product_description_length,
    product_photos_qty,
    weight_g,
    length_cm,
    height_cm,
    width_cm
FROM {{ ref('staging_products') }}
