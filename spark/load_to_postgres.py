from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    current_timestamp
)


# ============================================================
# CONFIGURATION
# ============================================================

RULES_PATH = (
    "/opt/spark/data/association_rules"
)

JDBC_URL = (
    "jdbc:postgresql://postgres:5432/instacart"
)

POSTGRES_PROPERTIES = {
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver"
}


# ============================================================
# CREATE SPARK SESSION
# ============================================================

spark = (
    SparkSession.builder
    .appName(
        "Load Association Rules to PostgreSQL"
    )
    .master(
        "spark://spark-master:7077"
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


print("=" * 70)
print(
    "LOADING ASSOCIATION RULES INTO POSTGRESQL"
)
print("=" * 70)


# ============================================================
# READ RULES
# ============================================================

print(
    "\nReading association rules..."
)

rules = (
    spark.read
    .parquet(
        RULES_PATH
    )
)


# ============================================================
# COUNT RULES
# ============================================================

input_count = rules.count()

print(
    f"Rules found: "
    f"{input_count:,}"
)


# ============================================================
# SELECT REQUIRED COLUMNS
# ============================================================

rules = rules.select(
    col("antecedent"),
    col(
        "antecedent_product_names"
    ).cast("string"),

    col("consequent"),
    col(
        "consequent_product_names"
    ).cast("string"),

    col("confidence").cast("double"),

    col("lift").cast("double"),

    col("support").cast("double")
)


# ============================================================
# ADD TIMESTAMP
# ============================================================

rules = rules.withColumn(
    "created_at",
    current_timestamp()
)


# ============================================================
# DISPLAY SAMPLE
# ============================================================

print(
    "\nFinal data:"
)

rules.show(
    10,
    truncate=False
)


# ============================================================
# WRITE TO POSTGRESQL
# ============================================================

print(
    "\nWriting rules to PostgreSQL..."
)

(
    rules.write
    .mode("overwrite")
    .jdbc(
        url=JDBC_URL,
        table="association_rules",
        properties=POSTGRES_PROPERTIES
    )
)


# ============================================================
# VERIFY
# ============================================================

final_count = rules.count()


print("=" * 70)
print(
    "POSTGRESQL LOAD COMPLETED SUCCESSFULLY"
)
print("=" * 70)

print(
    f"Rules loaded: "
    f"{final_count:,}"
)

print(
    "PostgreSQL table: association_rules"
)

print("=" * 70)


spark.stop()
