import os
import psycopg2
import snowflake.connector
from airflow.sdk import Asset, dag, task
from datetime import datetime


static_loaded = Asset("snowflake://ecommerce/bronze/static")

TABLES = ["customers", "products", "sellers", "geolocation"]


def _get_postgres_conn():
    return psycopg2.connect(
        host="postgres",
        port=5432,
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    )


def _get_snowflake_conn():
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        database=os.environ["SNOWFLAKE_DATABASE"],
        schema="bronze",
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        role=os.environ.get("SNOWFLAKE_ROLE", ""),
    )


@dag(
    dag_id="postgres_to_snowflake",
    schedule="@once",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["bronze", "static", "ingestion"],
)
def postgres_to_snowflake():

    @task()
    def check_already_loaded() -> dict:
        """Kiểm tra table nào đã có data trong Bronze."""
        conn = _get_snowflake_conn()
        cur = conn.cursor()
        loaded = {}
        for table in TABLES:
            cur.execute(f"SELECT COUNT(*) FROM ecommerce.bronze.{table}")
            count = cur.fetchone()[0]
            loaded[table] = count > 0
            print(f"[{table}] already loaded: {loaded[table]} ({count} rows)")
        cur.close()
        conn.close()
        return loaded

    @task(outlets=[static_loaded], execution_timeout=None)
    def load_static_tables(loaded: dict) -> None:
        """Load các tables chưa có data từ PostgreSQL vào Snowflake Bronze."""
        pg_conn = _get_postgres_conn()
        pg_cur = pg_conn.cursor()
        sf_conn = _get_snowflake_conn()
        sf_cur = sf_conn.cursor()

        for table in TABLES:
            if loaded.get(table):
                print(f"[{table}] Skipping — already loaded.")
                continue

            # Lấy column names từ PostgreSQL
            pg_cur.execute(f"""
                SELECT column_name FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = '{table}'
                ORDER BY ordinal_position
            """)
            columns = [row[0] for row in pg_cur.fetchall()]
            columns_str = ", ".join(columns)
            placeholders = ", ".join(["%s"] * len(columns))

            # Đọc toàn bộ data từ PostgreSQL
            pg_cur.execute(f"SELECT {columns_str} FROM public.{table}")
            rows = pg_cur.fetchall()
            print(f"[{table}] {len(rows)} rows fetched from PostgreSQL.")

            if not rows:
                print(f"[{table}] No data found, skipping.")
                continue

            # Insert vào Snowflake Bronze theo batch
            sf_columns = columns_str + ", _op, _ingested_at"
            sf_placeholders = placeholders + ", %s, CURRENT_TIMESTAMP()"
            insert_sql = f"""
                INSERT INTO ecommerce.bronze.{table} ({sf_columns})
                VALUES ({sf_placeholders})
            """
            batch = [row + ("c",) for row in rows]
            batch_size = 10000
            for i in range(0, len(batch), batch_size):
                chunk = batch[i:i + batch_size]
                sf_cur.executemany(insert_sql, chunk)
                sf_conn.commit()
                print(f"[{table}] Inserted rows {i} - {i + len(chunk)}")
            print(f"[{table}] Done — {len(rows)} rows loaded.")

        pg_cur.close()
        pg_conn.close()
        sf_cur.close()
        sf_conn.close()

    loaded = check_already_loaded()
    load_static_tables(loaded)


postgres_to_snowflake()
