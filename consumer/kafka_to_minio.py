import json
import os
import io
from datetime import datetime

import boto3
import pandas as pd
from confluent_kafka import Consumer, KafkaError
from dotenv import load_dotenv

load_dotenv()

KAFKA_BROKER    = "localhost:9092"
KAFKA_GROUP_ID  = "minio-writer"
TOPICS          = [
    "ecommerce.public.orders",
    "ecommerce.public.order_items",
    "ecommerce.public.order_payments",
    "ecommerce.public.order_reviews",
]

MINIO_ENDPOINT  = "http://localhost:9000"
MINIO_BUCKET    = os.getenv("MINIO_BUCKET")
BATCH_SIZE      = 10

def get_s3_client(): #lấy địa chỉ, thông tin truy cập vào minio bucket
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=os.getenv("MINIO_ROOT_USER"),
        aws_secret_access_key=os.getenv("MINIO_ROOT_PASSWORD"),
    )

def parse_message(msg_value): #chuyển msg_value thành record. msg_value có dạng {"op":"...", "before": {1 row}, "after":{1 row}}
                                        #trả về record có dạng {1 row chứa thêm "op" và "ingested_at"}
    data = json.loads(msg_value)
    op = data.get("op")

    if op == "d":
        record = data.get("before")
    else:
        record = data.get("after")

    if record is None:
        return None

    record["_op"]          = op
    record["_ingested_at"] = datetime.utcnow().isoformat()
    return record

def write_parquet_to_minio(s3_client, records, topic): #records là list of record có dạng [{},{}] tạo từ buffer ở def consume(), function chuyển records thành fie parquet và nhét vào minio
    table_name = topic.split(".")[-1]
    timestamp  = datetime.utcnow().strftime("%Y%m%d_%H%M%S_%f")
    s3_key     = f"{table_name}/{table_name}_{timestamp}.parquet"

    df     = pd.DataFrame(records)
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False, engine="pyarrow")
    buffer.seek(0)

    s3_client.put_object(
        Bucket=MINIO_BUCKET,
        Key=s3_key,
        Body=buffer.getvalue(),
    )
    print(f"Written {len(records)} records → s3://{MINIO_BUCKET}/{s3_key}")

def consume(): #đăng kí consumer minio_writer trong kafka, nó nhận msg từ các topic và viết vào minio theo fuction write_parquet_to_minio
    print("Connecting to MinIO...")
    s3_client = get_s3_client()

    print("Connecting to Kafka...")
    conf = {
        "bootstrap.servers": KAFKA_BROKER,
        "group.id": KAFKA_GROUP_ID,
        "auto.offset.reset": "earliest",
    }
    consumer = Consumer(conf)
    consumer.subscribe(TOPICS)
    print(f"Subscribed to: {TOPICS}")

    print("Connected. Waiting for messages...")
    buffers = {topic: [] for topic in TOPICS}
    message_count = 0

    try:
        while True:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(f"Error: {msg.error()}")
                    break

            topic = msg.topic()
            value = msg.value().decode("utf-8")
            message_count += 1
            print(f"[{message_count}] Received: {topic} offset={msg.offset()}")

            record = parse_message(value)
            if record is None:
                continue

            buffers[topic].append(record)

            if len(buffers[topic]) >= BATCH_SIZE:
                write_parquet_to_minio(s3_client, buffers[topic], topic)
                buffers[topic] = []

    except KeyboardInterrupt:
        print("Stopped. Flushing remaining buffers...")
    finally:
        for topic, records in buffers.items():
            if records:
                print(f"Flushing {len(records)} remaining records for {topic}")
                write_parquet_to_minio(s3_client, records, topic)
        consumer.close()

if __name__ == "__main__":
    consume()