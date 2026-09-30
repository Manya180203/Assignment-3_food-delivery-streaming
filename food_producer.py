from kafka import KafkaProducer
import csv
import json
import time

# Connect to Kafka
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# Kafka topic name
topic_name = "food_orders"

# Start message
print("Starting Food Delivery Producer...")
print(f"Sending messages to Kafka topic: {topic_name}\n")

# Read the CSV file
with open("food_delivery_data.csv", "r", encoding="utf-8-sig") as file:

    csv_reader = csv.DictReader(file, delimiter=";")

    # Send each row from the CSV file to Kafka
    for row in csv_reader:

        # Send message to Kafka
        producer.send(topic_name, value=row)

        # Display the message in the terminal
        print("Sent:", json.dumps(row))

        # Wait 1 second to simulate real-time streaming
        time.sleep(1)

# Ensure all messages are sent
producer.flush()

# Close the Kafka connection
producer.close()

print("\nAll food delivery messages sent successfully!")