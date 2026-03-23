import json
import os
import pandas as pd
from kafka import KafkaConsumer
from snowflake.connector import connect
from snowflake.connector.pandas_tools import write_pandas
from dotenv import load_dotenv
from datetime import datetime, timezone

load_dotenv(override=False)

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
TOPIC = "txn.raw"
GROUP_ID = "txn-consumer-group"
BATCH_SIZE = 500

COLUMN_MAP = {
    "TransactionID":    "transaction_id",
    "isFraud":          "i_fraud",
    "TransactionDT":    "transaction_dt",
    "TransactionAmt":   "transaction_amt",
    "ProductCD":        "product_cd",
    "card1":            "card1",
    "card2":            "card2",
    "card3":            "card3",
    "card4":            "card4",
    "card5":            "card5",
    "card6":            "card6",
    "addr1":            "addr1",
    "addr2":            "addr2",
    "dist1":            "dist1",
    "dist2":            "dist2",
    "P_emaildomain":    "p_emaildomain",
    "R_emaildomain":    "r_emaildomain",
    "C1":  "c1",  "C2":  "c2",  "C3":  "c3",  "C4":  "c4",
    "C5":  "c5",  "C6":  "c6",  "C7":  "c7",  "C8":  "c8",
    "C9":  "c9",  "C10": "c10", "C11": "c11", "C12": "c12",
    "C13": "c13", "C14": "c14",
    "D1":  "d1",  "D2":  "d2",  "D3":  "d3",  "D4":  "d4",
    "D5":  "d5",  "D6":  "d6",  "D7":  "d7",  "D8":  "d8",
    "D9":  "d9",  "D10": "d10", "D11": "d11", "D12": "d12",
    "D13": "d13", "D14": "d14", "D15": "d15",
    "M1":  "m1",  "M2":  "m2",  "M3":  "m3",  "M4":  "m4",
    "M5":  "m5",  "M6":  "m6",  "M7":  "m7",  "M8":  "m8",
    "M9":  "m9",
    "_ingested_at": "_ingested_at",
    "_source":      "_source",
}

SNOWFLAKE_COLS = list(COLUMN_MAP.values())


def get_snowflake_conn():
    return connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database="FINTECH_RAW_DB",
        schema="TRANSACTIONS",
        role="DATA_ENG_ROLE"
    )


def clean_batch(messages):
    rows = []
    for msg in messages:
        row = {}
        for src_col, tgt_col in COLUMN_MAP.items():
            val = msg.get(src_col)
            if val != val:
                val = None
            row[tgt_col] = val
        rows.append(row)

    df = pd.DataFrame(rows, columns=SNOWFLAKE_COLS)

    df["_ingested_at"] = pd.to_datetime(df["_ingested_at"], utc=True)
    df["_loaded_at"] = datetime.now(timezone.utc)

    return df


def load_to_snowflake(conn, df):
    df.columns = [c.upper() for c in df.columns]
    success, _, num_rows, _ = write_pandas(
    conn,
    df,
    "RAW_TRANSACTIONS",
    schema="TRANSACTIONS",
    auto_create_table=False,
    use_logical_type=True
    )
    if success:
        print(f"Loaded {num_rows} rows to Snowflake")
    else:
        print("Load failed")


def main():
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        group_id=GROUP_ID,
        auto_offset_reset="earliest",
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        enable_auto_commit=True
    )

    conn = get_snowflake_conn()
    print(f"Connected to Snowflake")
    print(f"Listening on topic: {TOPIC}")

    batch = []

    for message in consumer:
        batch.append(message.value)

        if len(batch) >= BATCH_SIZE:
            df = clean_batch(batch)
            load_to_snowflake(conn, df)
            batch = []

if __name__ == "__main__":
    main()