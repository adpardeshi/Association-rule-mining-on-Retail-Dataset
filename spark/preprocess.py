from pyspark.sql import SparkSession
from pyspark.sql.functions import collect_set, col, size


# ============================================================
# CREATE SPARK SESSION
# ============================================================

spark = (
    SparkSession.builder
    .appName("Instacart Preprocessing")
    .master("spark://spark-master:7077")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


print("=" * 70)
print("STARTING INSTACART DATA PREPROCESSING")
print("=" * 70)


# ============================================================
# FILE PATHS
# ============================================================

ORDERS_PATH = "/opt/spark/data/orders.csv"

ORDER_PRODUCTS_PATH = (
    "/opt/spark/data/order_products__prior.csv"
)

OUTPUT_PATH = (
    "/opt/spark/data/transactions.parquet"
)


# ============================================================
# READ ORDERS
# ============================================================

print("\nReading orders.csv...")

orders = spark.read.csv(
    ORDERS_PATH,
    header=True,
    inferSchema=True
)

print(
    f"Orders count: {orders.count():,}"
)


# ============================================================
# READ ORDER PRODUCTS
# ============================================================

print("\nReading order_products__prior.csv...")

order_products = spark.read.csv(
    ORDER_PRODUCTS_PATH,
    header=True,
    inferSchema=True
)

print(
    f"Order-product records: "
    f"{order_products.count():,}"
)


# ============================================================
# SELECT REQUIRED COLUMNS
# ============================================================

orders = orders.select(
    "order_id"
)

order_products = order_products.select(
    "order_id",
    "product_id"
)


# ============================================================
# JOIN DATASETS
# ============================================================

print("\nJoining orders and products...")

joined = orders.join(
    order_products,
    on="order_id",
    how="inner"
)


# ============================================================
# CREATE TRANSACTIONS
# ============================================================

print("\nCreating transaction baskets...")

transactions = (
    joined
    .groupBy("order_id")
    .agg(
        collect_set(
            col("product_id")
        ).alias("items")
    )
)


# ============================================================
# REMOVE INVALID TRANSACTIONS
# ============================================================

transactions = (
    transactions
    .filter(col("items").isNotNull())
    .filter(size(col("items")) >= 2)
)


# ============================================================
# SHOW SAMPLE
# ============================================================

print("\nSample transactions:")

transactions.show(
    10,
    truncate=False
)


# ============================================================
# TRANSACTION COUNT
# ============================================================

transaction_count = transactions.count()

print(
    f"\nFinal transaction count: "
    f"{transaction_count:,}"
)


# ============================================================
# WRITE PARQUET
# ============================================================

print("\nWriting transactions to Parquet...")

(
    transactions
    .write
    .mode("overwrite")
    .parquet(OUTPUT_PATH)
)


# ============================================================
# SUCCESS
# ============================================================

print("=" * 70)
print("PREPROCESSING COMPLETED SUCCESSFULLY")
print("=" * 70)

print(
    f"Output: {OUTPUT_PATH}"
)

print(
    f"Transactions created: "
    f"{transaction_count:,}"
)

print("=" * 70)


spark.stop()
