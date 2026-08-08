from pyspark.sql import SparkSession
from pyspark.sql.functions import concat_ws


# ============================================================
# CREATE SPARK SESSION
# ============================================================

spark = (
    SparkSession.builder
    .appName("Export Instacart Results")
    .master("spark://spark-master:7077")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


print("=" * 70)
print("EXPORTING FP-GROWTH RESULTS")
print("=" * 70)


# ============================================================
# PATHS
# ============================================================

FREQUENT_ITEMSETS_PATH = "/opt/spark/data/frequent_itemsets"
ASSOCIATION_RULES_PATH = "/opt/spark/data/association_rules"

FREQUENT_ITEMSETS_OUTPUT = "/opt/spark/data/results/frequent_itemsets_csv"
ASSOCIATION_RULES_OUTPUT = "/opt/spark/data/results/association_rules_csv"


# ============================================================
# LOAD FREQUENT ITEMSETS
# ============================================================

print("\nLoading frequent itemsets...")

frequent_itemsets = spark.read.parquet(
    FREQUENT_ITEMSETS_PATH
)

frequent_itemsets_count = frequent_itemsets.count()

print(f"Frequent itemsets count: {frequent_itemsets_count}")

print("\nFrequent itemsets schema:")
frequent_itemsets.printSchema()


# ============================================================
# LOAD ASSOCIATION RULES
# ============================================================

print("\nLoading association rules...")

association_rules = spark.read.parquet(
    ASSOCIATION_RULES_PATH
)

association_rules_count = association_rules.count()

print(f"Association rules count: {association_rules_count}")

print("\nAssociation rules schema:")
association_rules.printSchema()


# ============================================================
# SHOW SAMPLE RESULTS
# ============================================================

print("\n" + "=" * 70)
print("SAMPLE FREQUENT ITEMSETS")
print("=" * 70)

frequent_itemsets.show(
    10,
    truncate=False
)


print("\n" + "=" * 70)
print("SAMPLE ASSOCIATION RULES")
print("=" * 70)

association_rules.show(
    10,
    truncate=False
)


# ============================================================
# CONVERT ARRAY COLUMNS TO STRING
# ============================================================

print("\nConverting array columns to strings...")

# Frequent itemsets:
# items: ARRAY<INT> -> STRING

frequent_itemsets_csv = (
    frequent_itemsets
    .withColumn(
        "items",
        concat_ws(",", "items")
    )
)


# Association rules:
# antecedent: ARRAY<INT> -> STRING
# consequent: ARRAY<INT> -> STRING

association_rules_csv = (
    association_rules
    .withColumn(
        "antecedent",
        concat_ws(",", "antecedent")
    )
    .withColumn(
        "consequent",
        concat_ws(",", "consequent")
    )
)


# ============================================================
# EXPORT FREQUENT ITEMSETS
# ============================================================

print("\nExporting frequent itemsets to CSV...")

(
    frequent_itemsets_csv
    .coalesce(1)
    .write
    .mode("overwrite")
    .option("header", True)
    .csv(FREQUENT_ITEMSETS_OUTPUT)
)

print(
    "Frequent itemsets exported successfully."
)


# ============================================================
# EXPORT ASSOCIATION RULES
# ============================================================

print("\nExporting association rules to CSV...")

(
    association_rules_csv
    .coalesce(1)
    .write
    .mode("overwrite")
    .option("header", True)
    .csv(ASSOCIATION_RULES_OUTPUT)
)

print(
    "Association rules exported successfully."
)


# ============================================================
# FINAL SUMMARY
# ============================================================

print("\n" + "=" * 70)
print("EXPORT COMPLETED SUCCESSFULLY")
print("=" * 70)

print(
    f"Frequent itemsets exported : {frequent_itemsets_count}"
)

print(
    f"Association rules exported : {association_rules_count}"
)

print(
    f"\nFrequent itemsets CSV:"
    f"\n{FREQUENT_ITEMSETS_OUTPUT}"
)

print(
    f"\nAssociation rules CSV:"
    f"\n{ASSOCIATION_RULES_OUTPUT}"
)

print("\n" + "=" * 70)


# ============================================================
# STOP SPARK
# ============================================================

spark.stop()
