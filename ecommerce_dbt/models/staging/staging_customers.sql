SELECT
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix            AS zip_code,
    INITCAP(customer_city)              AS city,
    UPPER(customer_state)               AS state
FROM {{ source('bronze', 'customers') }}
