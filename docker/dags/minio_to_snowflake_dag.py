import os
import tempfile
import boto3
import snowflake.connector
from airflow.sdk import Asset, dag, task
from datetime import datetime


bronze_updated = Asset("snowflake://ecommerce/bronze")
TABLES = ["orders", "order_items", "order_payments", "order_reviews"]
MAX_FILES_PER_RUN = 2000

def _get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=os.environ["MINIO_ENDPOINT"],
        aws_access_key_id=os.environ["MINIO_ACCESS_KEY"],
        aws_secret_access_key=os.environ["MINIO_SECRET_KEY"],
    )

def _get_snowflake_conn():
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        database=os.environ["SNOWFLAKE_DATABASE"],
        schema=os.environ["SNOWFLAKE_SCHEMA"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        role=os.environ.get("SNOWFLAKE_ROLE", ""),
    )

def _list_new_files(s3_client, bucket: str, table: str, last_loaded: str) -> list[str]:
    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=f"{table}/")
    files = []
    for obj in response.get("Contents", []):
        key = obj["Key"]
        modified = obj["LastModified"].strftime("%Y%m%d_%H%M%S")
        if modified > last_loaded:
            files.append(key)
    return sorted(files)

@dag(
    dag_id="minio_to_snowflake",
    schedule="*/15 * * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["bronze", "ingestion"],
)
def minio_to_snowflake():

    @task()
    def get_last_loaded_timestamps() -> dict:
        conn = _get_snowflake_conn()
        cur = conn.cursor()
        timestamps = {}
        for table in TABLES:
            cur.execute(f"SELECT MAX(_ingested_at) FROM ecommerce.bronze.{table}")
            result = cur.fetchone()[0]
            if result:
                timestamps[table] = result[:19].replace("-", "").replace("T", "_").replace(":", "")
            else:
                timestamps[table] = "00000000_000000"
        cur.close()
        conn.close()
        return timestamps

    @task(outlets=[bronze_updated])
    def load_tables(timestamps: dict) -> None:
        s3_client = _get_s3_client()
        bucket = os.environ["MINIO_BUCKET"]
        conn = _get_snowflake_conn()
        cur = conn.cursor()

        for table in TABLES:
            last_loaded = timestamps.get(table, "00000000_000000")
            files = _list_new_files(s3_client, bucket, table, last_loaded)[:MAX_FILES_PER_RUN]

            if not files:
                print(f"[{table}] No new files.")
                continue

            print(f"[{table}] {len(files)} new file(s) to load.")

            tmp_paths = []
            for key in files:
                obj = s3_client.get_object(Bucket=bucket, Key=key)
                with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
                    tmp.write(obj["Body"].read())
                    tmp_paths.append(tmp.name)

            for tmp_path in tmp_paths:
                cur.execute(f"PUT file://{tmp_path} @ecommerce.bronze.airflow_stage/{table}/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE")
                os.unlink(tmp_path)

            print(f"[{table}] All {len(files)} files uploaded to stage. Running bulk COPY INTO...")

            cur.execute(f"""
                COPY INTO ecommerce.bronze.{table}
                FROM @ecommerce.bronze.airflow_stage/{table}/
                FILE_FORMAT = (TYPE = PARQUET)
                MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
                PURGE = TRUE
            """)
            copy_result = cur.fetchall()
            rows_loaded = sum(r[3] for r in copy_result if r[3] is not None)
            print(f"[{table}] COPY done — {rows_loaded} rows loaded across {len(copy_result)} file(s).")

        cur.close()
        conn.close()

    timestamps = get_last_loaded_timestamps()
    load_tables(timestamps)


minio_to_snowflake()




