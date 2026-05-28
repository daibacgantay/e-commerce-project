from datetime  import datetime
import csv
from pathlib import Path
from psycopg2.extras import execute_values
import time
from db import get_connection

DATA_DIR = Path(__file__).parent.parent / "data"
SPEED = 7200

def parse_dt(value):#convert string to timestamp
    if not value:
        return None
    return datetime.strptime(value, "%Y-%m-%d %H:%M:%S") 

def load_orders():# load csv as dict. return list of dict. [{row1}, {row2},...]
    orders = []
    with open(DATA_DIR/ "olist_orders_dataset.csv", encoding = "utf-8-sig")as f:
        reader = csv.DictReader(f)
        for row in reader:
            orders.append({
                "order_id": row["order_id"],
                "customer_id": row["customer_id"],
                "order_status": row["order_status"],
                "order_purchase_timestamp": parse_dt(row["order_purchase_timestamp"]),
                "order_approved_at": parse_dt(row["order_approved_at"]),
                "order_delivered_carrier_date": parse_dt(row["order_delivered_carrier_date"]),
                "order_delivered_customer_date": parse_dt(row["order_delivered_customer_date"]),
                "order_estimated_delivery_date": parse_dt(row["order_estimated_delivery_date"]),
            })
        return orders
    
def load_grouped(filename, columns):#load csv file, return as {oid:[(),(),()]} 1 oid have many items, reviews, payments
    order_group = {}
    with open(DATA_DIR/ filename, encoding = "utf-8-sig")as f:
        reader = csv.DictReader(f)
        for row in reader:
            oid = row["order_id"]
            if oid not in order_group:
                order_group[oid] = []
            order_group[oid].append(tuple(row[col] or None for col in columns))
    return order_group
             

def is_valid_order(order):#check order valid or not. return boolean
    ts = [
        order["order_purchase_timestamp"],
        order["order_approved_at"],
        order["order_delivered_carrier_date"],
        order["order_delivered_customer_date"],
    ]

    filled = []
    for t in ts:
        if t is not None:
            filled.append(t)
    return filled == sorted(filled)

def build_event_timeline(orders, items, payments, reviews):
    # create list of event sorted by timestamp. return[(timestamp,action,{order_row}),(),()] if create new order. [(timestamp,action,[(),(),()]),(),()]
    event = []
    for order in orders:
        oid = order["order_id"]
        if is_valid_order(order) is False:
            continue
        event.append((
                order["order_purchase_timestamp"],
                "order purchased",
                order
            ))
        if order["order_approved_at"] is not None:
            event.append((
                order["order_approved_at"],
                "order approved",
                order
            ))
            event.append((
                order["order_approved_at"],
                "insert items",
                items.get(oid, [])
            ))
            event.append((
                order["order_approved_at"],
                "insert payment",
                payments.get(oid, [])
            ))
        if order["order_delivered_carrier_date"] is not None:
            event.append((
                order["order_delivered_carrier_date"],
                "carrier delivered",
                order
            ))
        if order["order_delivered_customer_date"] is not None:
            event.append((
                order["order_delivered_customer_date"],
                "delivered_succesfully",
                order,
            ))
            event.append((
                order["order_delivered_customer_date"],
                "reviews insert",
                reviews.get(oid, []),
            ))

    event.sort(key=lambda e: e[0])
    return event

def replay_events(events, connection):
    first_ts = events[0][0]
    start_time = datetime.now()
    with connection.cursor() as cur:
        cur.execute("""TRUNCATE orders, order_items, order_payments, order_reviews;""")
        for ts, event_type, data in events:
            real_offset = (ts - first_ts).total_seconds()
            sim_offset = real_offset/ SPEED
            sleep_time = sim_offset - (datetime.now()-start_time).total_seconds()
            if sleep_time > 0:
                time.sleep(sleep_time)
            
            if event_type == "order purchased":
                cur.execute("""
                INSERT INTO orders(order_id, customer_id, order_status, order_purchase_timestamp)
                VALUES (%s, %s, 'approving', %s)            
                            """,
                            (data["order_id"], data["customer_id"], data["order_purchase_timestamp"]))
                
            if event_type == "order approved":
                cur.execute("""
                UPDATE orders
                SET order_status = 'approved', order_approved_at = %s
                WHERE order_id = %s           
                            """,
                            (data["order_approved_at"], data["order_id"]))
                
            if event_type == "insert items":
                if data:
                    execute_values(cur, """
                    INSERT INTO order_items(order_id, order_item_id, product_id, seller_id, shipping_limit_date, price, freight_value)
                    VALUES %s          
                                """,
                                (data))
                
            if event_type == "insert payment":
                execute_values(cur,"""
                INSERT INTO order_payments(order_id, payment_sequential, payment_type, payment_installments, payment_value )
                VALUES %s          
                            """,
                            (data))
                
            if event_type == "carrier delivered":
                cur.execute("""
                UPDATE orders
                SET order_status = 'shipping', order_delivered_carrier_date = %s, order_estimated_delivery_date = %s
                WHERE order_id = %s           
                            """,
                            (data["order_delivered_carrier_date"],data["order_estimated_delivery_date"], data["order_id"]))
                
            if event_type == "delivered_succesfully":
                cur.execute("""
                UPDATE orders
                SET order_status = 'delivered', order_delivered_customer_date = %s
                WHERE order_id = %s           
                            """,
                            (data["order_delivered_customer_date"], data["order_id"],))
                
            if event_type == "reviews insert":
                execute_values(cur, """
                INSERT INTO order_reviews(review_id, order_id, review_score, review_comment_title, review_comment_message, review_creation_date, review_answer_timestamp )
                VALUES %s          
                            """,
                            (data))
            
            connection.commit()
            oid = data.get("order_id") if isinstance(data, dict) else "bulk"
            print(f"{ts} | {event_type} | {oid}")
            
if __name__ == "__main__":
    connection = get_connection()
    orders   = load_orders()
items    = load_grouped("olist_order_items_dataset.csv",
               ["order_id","order_item_id","product_id","seller_id",
                "shipping_limit_date","price","freight_value"])

payments = load_grouped("olist_order_payments_dataset.csv",
               ["order_id","payment_sequential","payment_type",
                "payment_installments","payment_value"])

reviews  = load_grouped("olist_order_reviews_dataset.csv",
               ["review_id","order_id","review_score","review_comment_title",
                "review_comment_message","review_creation_date","review_answer_timestamp"])
events   = build_event_timeline(orders, items, payments, reviews)
replay_events(events, connection)


            
            
