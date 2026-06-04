CREATE DATABASE IF NOT EXISTS ecommerce;

CREATE SCHEMA IF NOT EXISTS ecommerce.bronze;
CREATE SCHEMA IF NOT EXISTS ecommerce.silver;
CREATE SCHEMA IF NOT EXISTS ecommerce.gold;




-- Xóa tất cả tables trong Silver
DROP SCHEMA ECOMMERCE.SILVER CASCADE;
CREATE SCHEMA ECOMMERCE.SILVER;

-- Xóa tất cả tables trong Gold
DROP SCHEMA ECOMMERCE.GOLD CASCADE;
CREATE SCHEMA ECOMMERCE.GOLD;

-- ------------------------------------------------------------
-- 2. Bronze Tables  (raw, all VARCHAR — no type coercion here)
--    Extra columns: _op (c/u/d), _ingested_at (ISO timestamp)
-- ------------------------------------------------------------

CREATE OR REPLACE TABLE ecommerce.bronze.orders (
    order_id                          VARCHAR,
    customer_id                       VARCHAR,
    order_status                      VARCHAR,
    order_purchase_timestamp          VARCHAR,
    order_approved_at                 VARCHAR,
    order_delivered_carrier_date      VARCHAR,
    order_delivered_customer_date     VARCHAR,
    order_estimated_delivery_date     VARCHAR,
    _op                               VARCHAR,
    _ingested_at                      VARCHAR
);

CREATE OR REPLACE TABLE ecommerce.bronze.order_items (
    order_id                VARCHAR,
    order_item_id           VARCHAR,
    product_id              VARCHAR,
    seller_id               VARCHAR,
    shipping_limit_date     VARCHAR,
    price                   VARCHAR,
    freight_value           VARCHAR,
    _op                     VARCHAR,
    _ingested_at            VARCHAR
);

CREATE OR REPLACE TABLE ecommerce.bronze.order_payments (
    order_id                VARCHAR,
    payment_sequential      VARCHAR,
    payment_type            VARCHAR,
    payment_installments    VARCHAR,
    payment_value           VARCHAR,
    _op                     VARCHAR,
    _ingested_at            VARCHAR
);

CREATE OR REPLACE TABLE ecommerce.bronze.order_reviews (
    review_id               VARCHAR,
    order_id                VARCHAR,
    review_score            VARCHAR,
    review_comment_title    VARCHAR,
    review_comment_message  VARCHAR,
    review_creation_date    VARCHAR,
    review_answer_timestamp VARCHAR,
    _op                     VARCHAR,
    _ingested_at            VARCHAR
);
CREATE OR REPLACE TABLE ecommerce.bronze.customers (
    customer_id                 VARCHAR,
    customer_unique_id          VARCHAR,
    customer_zip_code_prefix    VARCHAR,
    customer_city               VARCHAR,
    customer_state              VARCHAR,
    _op                         VARCHAR,
    _ingested_at                VARCHAR
);

CREATE OR REPLACE TABLE ecommerce.bronze.products (
    product_id                      VARCHAR,
    product_category_name           VARCHAR,
    product_name_length             VARCHAR,
    product_description_length      VARCHAR,
    product_photos_qty              VARCHAR,
    product_weight_g                VARCHAR,
    product_length_cm               VARCHAR,
    product_height_cm               VARCHAR,
    product_width_cm                VARCHAR,
    _op                             VARCHAR,
    _ingested_at                    VARCHAR
);

CREATE OR REPLACE TABLE ecommerce.bronze.sellers (
    seller_id                   VARCHAR,
    seller_zip_code_prefix      VARCHAR,
    seller_city                 VARCHAR,
    seller_state                VARCHAR,
    _op                         VARCHAR,
    _ingested_at                VARCHAR
);

CREATE OR REPLACE TABLE ecommerce.bronze.geolocation (
    geolocation_zip_code_prefix VARCHAR,
    geolocation_lat             VARCHAR,
    geolocation_lng             VARCHAR,
    geolocation_city            VARCHAR,
    geolocation_state           VARCHAR,
    _op                         VARCHAR,
    _ingested_at                VARCHAR
);

-- ------------------------------------------------------------
-- 3. Internal Stage (Airflow sẽ PUT file parquet lên đây)
-- ------------------------------------------------------------
CREATE OR REPLACE STAGE ecommerce.bronze.airflow_stage
    FILE_FORMAT = (TYPE = PARQUET);


-- ------------------------------------------------------------
-- 4. Warehouse (bỏ qua nếu đã có)
-- ------------------------------------------------------------
CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH
    WAREHOUSE_SIZE = XSMALL
    AUTO_SUSPEND   = 60
    AUTO_RESUME    = TRUE;