# E-Commerce Modern Data Stack

A production-ready data pipeline built on the Brazilian Olist e-commerce dataset, simulating real-time streaming via CDC (Change Data Capture).

## Architecture

<img src="assets/Blank diagram.png" width="800"/>

## Tech Stack

| Layer | Tool |
|---|---|
| Source Database | PostgreSQL |
| CDC | Debezium + Kafka KRaft (no Zookeeper) |
| Schema Registry | Confluent Schema Registry |
| Data Lake | MinIO |
| Orchestration | Airflow 3.0 |
| Data Warehouse | Snowflake |
| Transformations | dbt |
| CI/CD | GitHub Actions |

## Dataset

[Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) — 100k orders from 2016–2018.

**Streaming simulation strategy:**
- Static tables (`customers`, `products`, `sellers`) → INSERT once
- `orders` → replays CDC events based on timestamps:
  - `order_purchase_timestamp` → INSERT (`op: c`, status: created)
  - `order_approved_at` → UPDATE (`op: u`, status: approved)
  - `order_delivered_carrier_date` → UPDATE (`op: u`, status: shipped)
  - `order_delivered_customer_date` → UPDATE (`op: u`, status: delivered)

## dbt Models

```
staging/        → ephemeral, cleans raw data from Snowflake Bronze
intermediate/   → tables in Silver schema, business logic
marts/
  dimensions/   → dim_customers, dim_products, dim_sellers, dim_dates
  facts/        → fct_orders, fct_order_items, fct_payments, fct_reviews
```

## Prerequisites

- Docker & Docker Compose
- Python 3.11+
- Snowflake account
- dbt CLI (`pip install dbt-snowflake`)

## Getting Started

**1. Clone the repo**
```bash
git clone https://github.com/daibacgantay/e-commerce-project.git
cd e-commerce-project
```

**2. Configure environment**
```bash
cp .env.example .env
# Edit .env with your Snowflake credentials
```

**3. Start infrastructure**
```bash
docker compose up -d
```

**4. Load data & start streaming simulation**
```bash
cd data-loader
python loader.py           # load static tables
python stream_simulate.py  # simulate CDC events
```

**5. Register Debezium connector**
```bash
cd kafka-debezium
python register_connector.py
```

**6. Install dbt packages & run transformations**
```bash
cd ecommerce_dbt
dbt deps
dbt run
dbt test
```

## Environment Variables

See `.env.example` for all required variables. Key ones:

```
SNOWFLAKE_ACCOUNT=
SNOWFLAKE_USER=
SNOWFLAKE_PASSWORD=
SNOWFLAKE_ROLE=
SNOWFLAKE_WAREHOUSE=
```

## CI/CD

- **CI** — triggers on every push: Python lint (`flake8`) + `dbt compile`
- **CD** — triggers on merge to `main`: `dbt run` + `dbt test` on Snowflake production

## Project Structure

```
e-commerce-project/
├── data/                  # Olist CSV files
├── postgres/              # Source schema DDL
├── data-loader/           # Static load + streaming simulation
├── kafka-debezium/        # Debezium connector registration
├── consumer/              # Kafka → MinIO consumer
├── docker/dags/           # Airflow DAGs
├── ecommerce_dbt/         # dbt project
│   ├── models/
│   │   ├── staging/
│   │   ├── intermediate/
│   │   └── marts/
│   └── tests/
├── snowflake/             # Snowflake setup SQL
├── .github/workflows/     # CI/CD
├── docker-compose.yml
└── .env.example
```
