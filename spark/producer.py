from pyspark.sql import SparkSession
from kafka import KafkaProducer
import json

spark = (
    SparkSession.builder
    .appName("Spark Kafka Producer")
    .master("spark://spark-master:7077")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("Reading transactions...")

df = spark.read.parquet("/opt/spark/data/transactions.parquet")

producer = KafkaProducer(
    bootstrap_servers="kafka:9092",
    value_serializer=lambda x: json.dumps(x).encode("utf-8")
)

count = 0

for row in df.toLocalIterator():

    producer.send(
        "orders",
        {
            "order_id": row.order_id,
            "items": row.items
        }
    )

    count += 1

    if count % 100000 == 0:
        print(f"{count} transactions sent...")

producer.flush()

print(f"\nFinished sending {count} transactions.")

spark.stop()
