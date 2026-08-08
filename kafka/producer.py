import json
import time
import pandas as pd
from kafka import KafkaProducer

# ============================================================
# CONFIGURATION
# ============================================================

KAFKA_SERVER = "localhost:29092"
TOPIC = "instacart-transactions"

DATA_PATH = "data/transactions.parquet"

# ============================================================
# CREATE KAFKA PRODUCER
# ============================================================

print("Connecting to Kafka...")

producer = KafkaProducer(
    bootstrap_servers=[KAFKA_SERVER],
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    acks="all",
    retries=5,
    linger_ms=10,
    batch_size=16384
)

print("Connected to Kafka successfully!")

# ============================================================
# LOAD TRANSACTIONS
# ============================================================

print("Loading transactions...")

df = pd.read_parquet(DATA_PATH)

print(f"Total transactions to send: {len(df)}")

# ============================================================
# SEND TRANSACTIONS TO KAFKA
# ============================================================

print("Starting Kafka producer...")
print(f"Topic: {TOPIC}")
print("=" * 60)

start_time = time.time()

for index, row in df.iterrows():

    message = {
        "order_id": int(row["order_id"]),
        "items": [int(x) for x in row["items"]]
    }

    producer.send(
        TOPIC,
        value=message
    )

    # Show progress every 10,000 transactions
    if (index + 1) % 10000 == 0:
        producer.flush()

        elapsed = time.time() - start_time
        rate = (index + 1) / elapsed if elapsed > 0 else 0

        print(
            f"Sent: {index + 1:,} / {len(df):,} "
            f"| Rate: {rate:,.0f} transactions/sec"
        )

# ============================================================
# FLUSH REMAINING MESSAGES
# ============================================================

print("Flushing remaining messages...")

producer.flush()

# ============================================================
# CLOSE PRODUCER
# ============================================================

producer.close()

elapsed = time.time() - start_time

print("=" * 60)
print("Kafka Producer completed successfully!")
print(f"Total transactions sent: {len(df):,}")
print(f"Total time: {elapsed / 60:.2f} minutes")
print("=" * 60)