import csv, os
from pathlib import Path
from db import get_connection
from psycopg2.extras import execute_values
from dotenv import load_dotenv

DATA_DIR = Path(__file__).parent.parent / "data"


def load_static_table(connection): #load data to database main function
    tables = [
        {
            "file": "olist_customers_dataset.csv",
            "table": "customers",
            "columns": ["customer_id","customer_unique_id","customer_zip_code_prefix","customer_city","customer_state"],
            "has_pk": True
        },
        {
            "file": "olist_geolocation_dataset.csv",
            "table": "geolocation",
            "columns": ["geolocation_zip_code_prefix","geolocation_lat","geolocation_lng","geolocation_city","geolocation_state"],
            "has_pk": False
        },
        {
            "file": "olist_products_dataset.csv",
            "table": "products",
            "columns": ["product_id","product_category_name","product_name_length","product_description_length","product_photos_qty","product_weight_g","product_length_cm","product_height_cm","product_width_cm"],
            "has_pk": True
        },
        {
            "file": "olist_sellers_dataset.csv",
            "table": "sellers",
            "columns": ["seller_id","seller_zip_code_prefix","seller_city","seller_state"],
            "has_pk": True
        },
        {
            "file": "product_category_name_translation.csv",
            "table": "product_category_translation",
            "columns": ["product_category_name","product_category_name_english"],
            "has_pk": True
        }
    ]
    with connection.cursor() as cur:
        for t in tables:
            rows = read_csv(t["file"], t["columns"])
            upsert(cur, t["table"], t["columns"], rows, t["has_pk"])
            connection.commit()
            print(f"Loaded {len(rows)} rows to {t["table"]} table")

def read_csv(filename, columns): #read csv and return list of tuple [(a,b,c), (d,e,f),...]
    data_tuple = []
    with open(DATA_DIR/filename, encoding='utf-8-sig') as file:
        reader = csv.DictReader(file)
        for i in reader:
            data_tuple.append(tuple(i[col] or None for col in columns))
    return data_tuple


def upsert(cur, table, columns, rows, has_pk): #wrtite ddl and execute
    # Bước 1 — build các phần của câu SQL
    cols         = ", ".join(columns)
    placeholders = ", ".join(["%s"] * len(columns))

    # Bước 2 — ghép thành câu INSERT hoàn chỉnh
    sql = f"INSERT INTO {table} ({cols}) VALUES %s"

    # Bước 3 — thêm đuôi tùy theo has_pk
    if has_pk:
        sql += " ON CONFLICT DO NOTHING"
    else:
        cur.execute(f"TRUNCATE {table}")

    # Bước 4 — thực thi với toàn bộ rows
    execute_values(cur, sql, rows, page_size=1000)


if __name__ == "__main__":
    conn = get_connection()
    load_static_table(conn)
    conn.close()
