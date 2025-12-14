from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import *

spark = SparkSession.builder \
    .appName("KafkaToDeltaAmazonReviews") \
    .getOrCreate()

schema = StructType([
    StructField("rating", FloatType()),
    StructField("title", StringType()),
    StructField("text", StringType()),
    StructField("asin", StringType()),
    StructField("user_id", StringType()),
    StructField("timestamp", LongType())
])

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "amazon_reviews") \
    .option("startingOffsets", "earliest") \
    .load()

parsed = df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

query = parsed.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "/opt/datalake/checkpoints/amazon_reviews") \
    .start("/opt/datalake/bronze/amazon_reviews")

query.awaitTermination()
