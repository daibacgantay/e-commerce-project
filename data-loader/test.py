import csv, os
from pathlib import Path
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv



DATA_DIR = Path(__file__).parent.parent / "data"

def load_grouped(filename, columns):
    order_group = {}
    with open(DATA_DIR/ filename, encoding = "utf-8-sig")as f:
        reader = csv.DictReader(f)
        for row in reader:
            oid = row["order_id"]
            if oid not in order_group:
                order_group[oid] = []
            order_group[oid].append(tuple(row[col] or None for col in columns))
    return order_group

print(load_grouped("olist_order_items_dataset.csv",
               ["order_id","order_item_id","product_id","seller_id",
                "shipping_limit_date","price","freight_value"]))
    

