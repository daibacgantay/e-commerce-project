import requests
import time
import sys
import os
from dotenv import load_dotenv

load_dotenv()
CONNECT_URL     = "http://localhost:8083"
CONNECTOR_NAME  = "ecommerce-postgres-source"

def wait_for_connect(timeout=120):
    print("Waiting for kafka connect")
    start_time = time.time()
    while time.time() - start_time < timeout:
        try:
            r = requests.get(f"{CONNECT_URL}/connectors", timeout = 5)
            if r.status_code == 200:
                print("Kafka connect is ready")
                return
            
        except requests.exceptions.ConnectionError:
            pass
        time.sleep(5)
    print("Timeout: Kafka connect not ready")
    sys.exit(1)

CONNECTOR_CONFIG = {
    "name": CONNECTOR_NAME,
    "config": {
        # 1. Loại connector
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",

        # 2. Kết nối PostgreSQL (dùng hostname nội bộ Docker)
        "database.hostname": "postgres",
        "database.port":     "5432",
        "database.user":     os.getenv("POSTGRES_USER"),
        "database.password": os.getenv("POSTGRES_PASSWORD"),
        "database.dbname":   os.getenv("POSTGRES_DB"),

        # 3. Tên prefix cho Kafka topic
        # → tạo ra topic: ecommerce.public.orders, ecommerce.public.order_items, ...
        "topic.prefix": "ecommerce",

        # 4. Plugin WAL (pgoutput là built-in từ PG10, không cần cài thêm)
        "plugin.name": "pgoutput",

        # 5. Chỉ theo dõi các bảng CDC — bỏ qua bảng static
        "table.include.list": "public.orders,public.order_items,public.order_payments,public.order_reviews",

        # 6. Replication slot và publication (Debezium tự tạo)
        "slot.name":         "debezium_slot",
        "publication.name":  "debezium_publication",

        # 7. Snapshot: "never" vì data đã được load bằng stream_simulate.py
        "snapshot.mode": "never",

        # 8. Converter — khớp với docker-compose (JSON, không có schema)
        "key.converter":                    "org.apache.kafka.connect.json.JsonConverter",
        "value.converter":                  "org.apache.kafka.connect.json.JsonConverter",
        "key.converter.schemas.enable":     "false",
        "value.converter.schemas.enable":   "false",

        # 9. Gửi NUMERIC/DECIMAL dạng số thực thay vì Base64
        "decimal.handling.mode":            "double",
    }
}

def register():
    r = requests.get(f"{CONNECT_URL}/connectors/{CONNECTOR_NAME}")
    if r.status_code == 200:
        print(f"Connector '{CONNECTOR_NAME}' already exists. Deleting and re-registering...")
        requests.delete(f"{CONNECT_URL}/connectors/{CONNECTOR_NAME}")
        time.sleep(3)

    # Đăng ký connector mới
    r = requests.post(
        f"{CONNECT_URL}/connectors",
        json=CONNECTOR_CONFIG,
        headers={"Content-Type": "application/json"}
    )

    if r.status_code in (200, 201):
        print(f"Connector registered successfully.")
    else:
        print(f"Failed: {r.status_code} — {r.text}")
        sys.exit(1)


if __name__ == "__main__":
    wait_for_connect()
    register()