from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    desc,
    asc
)


# ============================================================
# CREATE SPARK SESSION
# ============================================================

spark = (
    SparkSession.builder
    .appName("Analyze Instacart Association Rules")
    .master("spark://spark-master:7077")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


print("=" * 70)
print("ANALYZING INSTACART ASSOCIATION RULES")
print("=" * 70)


# ============================================================
# PATH
# ============================================================

RULES_PATH = "/opt/spark/data/results/final_association_rules"


# ============================================================
# LOAD FINAL RULES
# ============================================================

print("\nLoading final association rules...")

rules = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(RULES_PATH)
)


# ============================================================
# SHOW SCHEMA
# ============================================================

print("\nSchema:")
rules.printSchema()


# ============================================================
# COUNT RULES
# ============================================================

total_rules = rules.count()

print("\nTotal Association Rules:", total_rules)


# ============================================================
# TOP 10 RULES BY CONFIDENCE
# ============================================================

print("\n")
print("=" * 70)
print("TOP 10 RULES BY CONFIDENCE")
print("=" * 70)

top_confidence = (
    rules
    .orderBy(desc("confidence"))
    .select(
        "antecedent_product_names",
        "consequent_product_names",
        "confidence",
        "lift"
    )
)

top_confidence.show(
    10,
    truncate=False
)


# ============================================================
# TOP 10 RULES BY LIFT
# ============================================================

print("\n")
print("=" * 70)
print("TOP 10 RULES BY LIFT")
print("=" * 70)

top_lift = (
    rules
    .orderBy(desc("lift"))
    .select(
        "antecedent_product_names",
        "consequent_product_names",
        "confidence",
        "lift"
    )
)

top_lift.show(
    10,
    truncate=False
)


# ============================================================
# RULES WITH LIFT > 2
# ============================================================

print("\n")
print("=" * 70)
print("STRONG ASSOCIATION RULES (LIFT > 2)")
print("=" * 70)

strong_rules = (
    rules
    .filter(col("lift") > 2)
    .orderBy(desc("lift"))
    .select(
        "antecedent_product_names",
        "consequent_product_names",
        "confidence",
        "lift"
    )
)

strong_rules.show(
    50,
    truncate=False
)


strong_count = strong_rules.count()

print(
    "\nNumber of rules with Lift > 2:",
    strong_count
)


# ============================================================
# RULES WITH CONFIDENCE > 0.25
# ============================================================

print("\n")
print("=" * 70)
print("HIGH CONFIDENCE RULES (CONFIDENCE > 25%)")
print("=" * 70)

high_confidence = (
    rules
    .filter(col("confidence") > 0.25)
    .orderBy(desc("confidence"))
    .select(
        "antecedent_product_names",
        "consequent_product_names",
        "confidence",
        "lift"
    )
)

high_confidence.show(
    50,
    truncate=False
)


high_confidence_count = high_confidence.count()

print(
    "\nNumber of rules with Confidence > 25%:",
    high_confidence_count
)


# ============================================================
# TOP RECOMMENDATION PRODUCTS
# ============================================================

print("\n")
print("=" * 70)
print("MOST COMMON CONSEQUENT PRODUCTS")
print("=" * 70)

top_recommendations = (
    rules
    .groupBy(
        "consequent_product_names"
    )
    .count()
    .orderBy(
        desc("count")
    )
)

top_recommendations.show(
    20,
    truncate=False
)


# ============================================================
# FINAL SUMMARY
# ============================================================

print("\n")
print("=" * 70)
print("ANALYSIS SUMMARY")
print("=" * 70)

print(
    "Total association rules:",
    total_rules
)

print(
    "Rules with Lift > 2:",
    strong_count
)

print(
    "Rules with Confidence > 25%:",
    high_confidence_count
)

print("=" * 70)


# ============================================================
# STOP SPARK
# ============================================================

spark.stop()
