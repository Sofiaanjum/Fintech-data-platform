import json
import os
import time
import pandas as pd
from kafka import KafkaProducer
from datetime import datetime

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
TOPIC = "txn.raw"
DATA_PATH = "data/raw/train_transaction.csv"
BATCH_SIZE = 100
SLEEP_SECONDS = 0.5

def serialize(data):
    return json.dumps(data).encode("utf-8")

def main():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=serialize
    )

    print(f"Reading {DATA_PATH}...")
    df = pd.read_csv(DATA_PATH, nrows=50000)
    df = df.where(pd.notnull(df), None)

    total = len(df)
    print(f"Total records: {total}")
    print(f"Publishing to topic: {TOPIC}")

    for i, row in df.iterrows():
        message = row.to_dict()
        message["_ingested_at"] = datetime.utcnow().isoformat()
        message["_source"] = "ieee_fraud_dataset"

        producer.send(TOPIC, value=message)

        if (i + 1) % BATCH_SIZE == 0:
            producer.flush()
            print(f"Sent {i + 1}/{total} records")
            time.sleep(SLEEP_SECONDS)

    producer.flush()
    print("Done -- all records published.")

if __name__ == "__main__":
    main()