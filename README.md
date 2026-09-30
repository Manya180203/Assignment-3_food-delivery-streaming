# Real-Time Food Delivery Analytics (Kafka → MySQL → Grafana)

## Overview
Streaming pipeline that sends food delivery orders through Kafka, stores them in MySQL and visualises them on a live Grafana dashboard.

## Architecture
CSV → food_producer.py → Kafka topic `food_orders` → consumer.py → MySQL (`food_delivery.orders`) → Grafana

## Setup
1. Start Kafka (localhost:9092) and MySQL.
2. Run `setup.sql` to create the database and table.
3. `pip install kafka-python mysql-connector-python`
4. Run `python consumer.py`, then `python food_producer.py`.
5. In Grafana add a MySQL data source (database `food_delivery`) and import `grafana_dashboard.json`.

## Files
- consumer.py – reads from Kafka and inserts into MySQL
- food_producer.py – streams CSV rows to Kafka (1 per second)
- setup.sql – table schema
- grafana_dashboard.json – dashboard export
