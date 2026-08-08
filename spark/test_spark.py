from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("Instacart Spark Test")
    .master("spark://spark-master:7077")
    .getOrCreate()
)

print("========================================")
print("Spark test started")
print("========================================")

data = [
    (1, "Milk"),
    (2, "Bread"),
    (3, "Eggs"),
]

df = spark.createDataFrame(
    data,
    ["product_id", "product_name"]
)

df.show()

print("Row count:", df.count())

print("========================================")
print("Spark test completed successfully")
print("========================================")

spark.stop()
