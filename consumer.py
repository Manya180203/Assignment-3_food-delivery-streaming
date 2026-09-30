from kafka import KafkaConsumer
import mysql.connector
from datetime import datetime
import json

# Connect to MySQL
conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="root",
    database="food_delivery"
)
cursor = conn.cursor()

print("Connected to MySQL!")

# Connect to Kafka
consumer = KafkaConsumer(
    "food_orders",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="food-delivery-consumer-2",
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

print("Connected to Kafka!")
print("Waiting for food delivery messages...\n")


# Convert blank values to None, otherwise to a number
def num(v):
    return float(v) if v not in (None, "") else None


sql = """INSERT INTO orders
(order_id, customer_id, restaurant_id, event_type, order_amount, delivery_fee,
 discount, tax, final_amount, distance_km, delivery_time_mins, customer_rating,
 location, timestamp)
VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""

# Read messages from Kafka
for message in consumer:

    d = message.value
    print("KEYS:", list(d.keys()))

    values = (
        d["order_id"],
        d["customer_id"],
        d["restaurant_id"],
        d["event_type"],
        num(d["order_amount"]),
        num(d["delivery_fee"]),
        num(d["discount"]),
        num(d["tax"]),
        num(d["final_amount"]),
        num(d["distance_km"]),
        num(d["delivery_time_mins"]),
        num(d["customer_rating"]),
        d["location"],
        datetime.strptime(d["timestamp"], "%d-%m-%y %H:%M")
    )

    # Insert the message into MySQL
    cursor.execute(sql, values)
    conn.commit()

    print("Saved to MySQL:", d)