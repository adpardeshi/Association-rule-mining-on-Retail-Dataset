from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    split,
    explode,
    collect_list,
    concat_ws,
    array_join
)


# ============================================================
# CREATE SPARK SESSION
# ============================================================

spark = (
    SparkSession.builder
    .appName("Enrich Instacart Association Rules")
    .master("spark://spark-master:7077")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


print("=" * 70)
print("ENRICHING ASSOCIATION RULES WITH PRODUCT NAMES")
print("=" * 70)


# ============================================================
# PATHS
# ============================================================

PRODUCTS_PATH = "/opt/spark/data/products.csv"

RULES_PATH = "/opt/spark/data/results/association_rules_csv"

OUTPUT_PATH = "/opt/spark/data/results/final_association_rules"


# ============================================================
# LOAD PRODUCTS
# ============================================================

print("\nLoading products...")

products = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(PRODUCTS_PATH)
    .select(
        "product_id",
        "product_name"
    )
)

print("\nProducts schema:")
products.printSchema()

print("\nSample products:")
products.show(
    5,
    truncate=False
)


# ============================================================
# LOAD ASSOCIATION RULES
# ============================================================

print("\nLoading association rules...")

rules = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(RULES_PATH)
)

print("\nAssociation rules schema:")
rules.printSchema()

print("\nSample association rules:")
rules.show(
    10,
    truncate=False
)


# ============================================================
# PREPARE PRODUCT MAPPING
# ============================================================

print("\nPreparing product ID mapping...")

product_mapping = (
    products
    .withColumn(
        "product_id_string",
        col("product_id").cast("string")
    )
)


# ============================================================
# CONVERT ANTECEDENT AND CONSEQUENT INTO ARRAYS
# ============================================================

print("\nConverting product ID strings into arrays...")

rules_prepared = (
    rules
    .withColumn(
        "antecedent_array",
        split(col("antecedent"), ",")
    )
    .withColumn(
        "consequent_array",
        split(col("consequent"), ",")
    )
)


# ============================================================
# MAP ANTECEDENT PRODUCT IDS TO PRODUCT NAMES
# ============================================================

print("\nMapping antecedent product IDs to product names...")

antecedent_names = (
    rules_prepared
    .select(
        "antecedent",
        "consequent",
        "confidence",
        "lift",
        explode("antecedent_array").alias(
            "product_id_string"
        )
    )
    .join(
        product_mapping,
        on="product_id_string",
        how="left"
    )
    .groupBy(
        "antecedent",
        "consequent",
        "confidence",
        "lift"
    )
    .agg(
        collect_list("product_name").alias(
            "antecedent_product_names"
        )
    )
)


# ============================================================
# MAP CONSEQUENT PRODUCT IDS TO PRODUCT NAMES
# ============================================================

print("\nMapping consequent product IDs to product names...")

consequent_names = (
    rules_prepared
    .select(
        "antecedent",
        "consequent",
        "confidence",
        "lift",
        explode("consequent_array").alias(
            "product_id_string"
        )
    )
    .join(
        product_mapping,
        on="product_id_string",
        how="left"
    )
    .groupBy(
        "antecedent",
        "consequent",
        "confidence",
        "lift"
    )
    .agg(
        collect_list("product_name").alias(
            "consequent_product_names"
        )
    )
)


# ============================================================
# JOIN ANTECEDENT AND CONSEQUENT NAMES
# ============================================================

print("\nCombining product names...")

final_rules = (
    antecedent_names
    .join(
        consequent_names,
        on=[
            "antecedent",
            "consequent",
            "confidence",
            "lift"
        ],
        how="left"
    )
)


# ============================================================
# CONVERT NAME ARRAYS INTO STRINGS
# ============================================================

final_rules = (
    final_rules
    .withColumn(
        "antecedent_product_names",
        concat_ws(
            ", ",
            "antecedent_product_names"
        )
    )
    .withColumn(
        "consequent_product_names",
        concat_ws(
            ", ",
            "consequent_product_names"
        )
    )
)


# ============================================================
# SELECT FINAL COLUMNS
# ============================================================

final_rules = final_rules.select(
    "antecedent",
    "antecedent_product_names",
    "consequent",
    "consequent_product_names",
    "confidence",
    "lift"
)


# ============================================================
# SHOW FINAL RESULTS
# ============================================================

print("\n" + "=" * 70)
print("FINAL BUSINESS-FRIENDLY ASSOCIATION RULES")
print("=" * 70)

final_rules.show(
    43,
    truncate=False
)


# ============================================================
# COUNT FINAL RULES
# ============================================================

final_count = final_rules.count()

print("\nTotal final association rules:", final_count)


# ============================================================
# EXPORT FINAL RESULTS
# ============================================================

print("\nExporting final association rules...")

(
    final_rules
    .coalesce(1)
    .write
    .mode("overwrite")
    .option("header", True)
    .csv(OUTPUT_PATH)
)


# ============================================================
# COMPLETION MESSAGE
# ============================================================

print("\n" + "=" * 70)
print("ENRICHMENT COMPLETED SUCCESSFULLY")
print("=" * 70)

print(
    "Final association rules:",
    final_count
)

print(
    "Output location:",
    OUTPUT_PATH
)

print("=" * 70)


# ============================================================
# STOP SPARK
# ============================================================

spark.stop()
