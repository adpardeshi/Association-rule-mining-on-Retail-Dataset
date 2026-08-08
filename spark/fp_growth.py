from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    size,
    explode,
    collect_set,
    concat_ws,
    round
)
from pyspark.ml.fpm import FPGrowth


# ============================================================
# CONFIGURATION
# ============================================================

TRANSACTIONS_PATH = (
    "/opt/spark/data/transactions.parquet"
)

PRODUCTS_PATH = (
    "/opt/spark/data/products.csv"
)

FREQUENT_ITEMSETS_PATH = (
    "/opt/spark/data/frequent_itemsets"
)

ASSOCIATION_RULES_PATH = (
    "/opt/spark/data/association_rules"
)

MIN_SUPPORT = 0.005

MIN_CONFIDENCE = 0.20

DISPLAY_LIMIT = 30


# ============================================================
# CREATE SPARK SESSION
# ============================================================

spark = (
    SparkSession.builder
    .appName("Instacart FP-Growth")
    .master("spark://spark-master:7077")
    .config("spark.executor.memory", "4g")
    .config("spark.driver.memory", "2g")
    .config("spark.sql.shuffle.partitions", "8")
    .config("spark.default.parallelism", "8")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


print("=" * 70)
print("STARTING SPARK FP-GROWTH")
print("=" * 70)


# ============================================================
# LOAD TRANSACTIONS
# ============================================================

print("\nLoading transactions...")

transactions = spark.read.parquet(
    TRANSACTIONS_PATH
)

transaction_count = transactions.count()

print(
    f"Total transactions: "
    f"{transaction_count:,}"
)


# ============================================================
# CLEAN TRANSACTIONS
# ============================================================

print("\nCleaning transactions...")

transactions_clean = (
    transactions
    .filter(col("items").isNotNull())
    .filter(size(col("items")) >= 2)
    .select(
        "order_id",
        "items"
    )
)


clean_transaction_count = (
    transactions_clean.count()
)


print(
    f"Transactions used for FP-Growth: "
    f"{clean_transaction_count:,}"
)


# ============================================================
# CACHE
# ============================================================

transactions_clean.cache()

transactions_clean.count()


# ============================================================
# FP-GROWTH
# ============================================================

print("\nStarting FP-Growth...")

fp_growth = FPGrowth(
    itemsCol="items",
    minSupport=MIN_SUPPORT,
    minConfidence=MIN_CONFIDENCE
)


model = fp_growth.fit(
    transactions_clean
)


print(
    "\nFP-Growth model trained successfully!"
)


# ============================================================
# FREQUENT ITEMSETS
# ============================================================

frequent_itemsets = (
    model.freqItemsets
    .withColumn(
        "item_count",
        size(col("items"))
    )
)


frequent_itemset_count = (
    frequent_itemsets.count()
)


print(
    f"Frequent itemsets: "
    f"{frequent_itemset_count:,}"
)


# ============================================================
# SAVE FREQUENT ITEMSETS
# ============================================================

print(
    "\nSaving frequent itemsets..."
)

(
    frequent_itemsets
    .write
    .mode("overwrite")
    .parquet(
        FREQUENT_ITEMSETS_PATH
    )
)


# ============================================================
# LOAD PRODUCT NAMES
# ============================================================

print(
    "\nLoading products.csv..."
)

products = spark.read.csv(
    PRODUCTS_PATH,
    header=True,
    inferSchema=True
).select(
    col("product_id").cast("int"),
    col("product_name")
)


# ============================================================
# CREATE PRODUCT NAME LOOKUP
# ============================================================

product_lookup = products


# ============================================================
# ASSOCIATION RULES
# ============================================================

print(
    "\nGenerating association rules..."
)

association_rules = (
    model.associationRules
)


# ============================================================
# EXPLODE ANTECEDENTS
# ============================================================

antecedent_names = (
    association_rules
    .select(
        "antecedent",
        explode(
            "antecedent"
        ).alias("product_id")
    )
    .join(
        product_lookup,
        "product_id",
        "left"
    )
    .groupBy(
        "antecedent"
    )
    .agg(
        collect_set(
            "product_name"
        ).alias(
            "antecedent_names"
        )
    )
)


# ============================================================
# EXPLODE CONSEQUENTS
# ============================================================

consequent_names = (
    association_rules
    .select(
        "consequent",
        explode(
            "consequent"
        ).alias("product_id")
    )
    .join(
        product_lookup,
        "product_id",
        "left"
    )
    .groupBy(
        "consequent"
    )
    .agg(
        collect_set(
            "product_name"
        ).alias(
            "consequent_names"
        )
    )
)


# ============================================================
# JOIN PRODUCT NAMES
# ============================================================

rules_with_names = (
    association_rules
    .join(
        antecedent_names,
        "antecedent",
        "left"
    )
    .join(
        consequent_names,
        "consequent",
        "left"
    )
)


# ============================================================
# CONVERT PRODUCT NAMES TO STRING
# ============================================================

final_rules = (
    rules_with_names
    .withColumn(
        "antecedent_product_names",
        concat_ws(
            ", ",
            col("antecedent_names")
        )
    )
    .withColumn(
        "consequent_product_names",
        concat_ws(
            ", ",
            col("consequent_names")
        )
    )
    .select(
        "antecedent",
        "antecedent_product_names",
        "consequent",
        "consequent_product_names",
        "confidence",
        "lift",
        "support"
    )
)


# ============================================================
# COUNT RULES
# ============================================================

association_rule_count = (
    final_rules.count()
)


print(
    f"\nAssociation rules generated: "
    f"{association_rule_count:,}"
)


# ============================================================
# DISPLAY TOP RULES
# ============================================================

print(
    "\nTop Association Rules:"
)

(
    final_rules
    .orderBy(
        col("lift").desc(),
        col("confidence").desc()
    )
    .select(
        "antecedent_product_names",
        "consequent_product_names",
        round(
            col("confidence"),
            4
        ).alias("confidence"),
        round(
            col("lift"),
            4
        ).alias("lift"),
        round(
            col("support"),
            4
        ).alias("support")
    )
    .show(
        DISPLAY_LIMIT,
        truncate=False
    )
)


# ============================================================
# SAVE FINAL RULES
# ============================================================

print(
    "\nSaving association rules..."
)

(
    final_rules
    .write
    .mode("overwrite")
    .parquet(
        ASSOCIATION_RULES_PATH
    )
)


# ============================================================
# FINAL SUMMARY
# ============================================================

print("=" * 70)
print("FP-GROWTH COMPLETED SUCCESSFULLY")
print("=" * 70)

print(
    f"Transactions: "
    f"{transaction_count:,}"
)

print(
    f"Frequent Itemsets: "
    f"{frequent_itemset_count:,}"
)

print(
    f"Association Rules: "
    f"{association_rule_count:,}"
)

print(
    f"Minimum Support: "
    f"{MIN_SUPPORT}"
)

print(
    f"Minimum Confidence: "
    f"{MIN_CONFIDENCE}"
)

print(
    f"Rules Output: "
    f"{ASSOCIATION_RULES_PATH}"
)

print("=" * 70)


transactions_clean.unpersist()

spark.stop()
