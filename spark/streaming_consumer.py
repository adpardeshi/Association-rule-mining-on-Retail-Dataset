from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    explode,
    count
)
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    ArrayType
)

# ============================================================
# CREATE SPARK SESSION
# ============================================================

spark = (
    SparkSession.builder
    .appName("Instacart Kafka Streaming")
    .master("spark://spark-master:7077")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("=" * 70)
print("STARTING INSTACART KAFKA STREAMING CONSUMER")
print("=" * 70)

# ============================================================
# KAFKA CONFIGURATION
# ============================================================

KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
KAFKA_TOPIC = "instacart-transactions"

print(f"Kafka Bootstrap Server: {KAFKA_BOOTSTRAP_SERVERS}")
print(f"Kafka Topic: {KAFKA_TOPIC}")

# ============================================================
# JSON SCHEMA
# ============================================================

schema = StructType([
    StructField(
        "order_id",
        IntegerType(),
        True
    ),
    StructField(
        "items",
        ArrayType(IntegerType()),
        True
    )
])

# ============================================================
# READ STREAM FROM KAFKA
# ============================================================

print("=" * 70)
print("CONNECTING TO KAFKA...")
print("=" * 70)

kafka_df = (
    spark.readStream
    .format("kafka")
    .option(
        "kafka.bootstrap.servers",
        KAFKA_BOOTSTRAP_SERVERS
    )
    .option(
        "subscribe",
        KAFKA_TOPIC
    )

    # IMPORTANT:
    # Read messages that are already present in Kafka
    .option(
        "startingOffsets",
        "earliest"
    )

    # Process a limited number of messages per micro-batch
    # This helps prevent overwhelming Spark
    .option(
        "maxOffsetsPerTrigger",
        10000
    )

    .load()
)

# ============================================================
# CONVERT KAFKA VALUE TO STRING
# ============================================================

json_df = (
    kafka_df
    .selectExpr(
        "CAST(value AS STRING) AS json_value"
    )
)

# ============================================================
# PARSE JSON
# ============================================================

transactions = (
    json_df
    .select(
        from_json(
            col("json_value"),
            schema
        ).alias("data")
    )
    .select(
        "data.order_id",
        "data.items"
    )
)

# ============================================================
# REMOVE INVALID RECORDS
# ============================================================

valid_transactions = (
    transactions
    .filter(
        col("order_id").isNotNull()
    )
    .filter(
        col("items").isNotNull()
    )
)

# ============================================================
# EXPLODE PRODUCTS
# ============================================================

products = (
    valid_transactions
    .select(
        "order_id",
        explode("items").alias("product_id")
    )
)

# ============================================================
# STREAMING OUTPUT
# ============================================================

print("=" * 70)
print("STARTING SPARK STRUCTURED STREAMING")
print("=" * 70)

query = (
    products
    .writeStream
    .format("console")
    .outputMode("append")
    .option(
        "truncate",
        False
    )
    .option(
        "numRows",
        20
    )
    .option(
        "checkpointLocation",
        "/tmp/instacart-kafka-checkpoint-v2"
    )
    .start()
)

print("=" * 70)
print("SPARK STREAMING CONSUMER STARTED SUCCESSFULLY!")
print("=" * 70)

print(f"Reading Kafka topic: {KAFKA_TOPIC}")
print(f"Kafka server: {KAFKA_BOOTSTRAP_SERVERS}")
print("Starting from: EARLIEST")
print("Waiting for streaming data...")
print("=" * 70)

query.awaitTermination()
