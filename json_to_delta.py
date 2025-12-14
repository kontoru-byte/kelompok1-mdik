from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("JSON to Delta Bronze") \
    .getOrCreate()

df = spark.read.json("/opt/spark-apps/data/json/sports_and_outdoors.jsonl").limit(10000)

df.write.format("delta") \
    .mode("overwrite") \
    .save("/opt/datalake/bronze/sports_and_outdoors")

spark.stop()

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("JSON to Delta") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

json_df = spark.read.json("/opt/spark-apps/data/json/sports_and_outdoors.jsonl")

# Rename kolom agar konsisten
json_df = json_df.selectExpr("asin as product_id", "user_id", "rating", "text as review_text")

# Repartition untuk aman memory
json_df = json_df.repartition(4)

# Tulis ke Bronze
json_df.write.format("delta").mode("overwrite").save("/opt/datalake/bronze/sports_and_outdoors")

spark.stop()
