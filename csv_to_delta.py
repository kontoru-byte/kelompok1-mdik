from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Ecommerce-CSV-to-Delta") \
    .getOrCreate()

df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("/opt/spark-apps/data/csv/ecomdataset.csv")
    
df.write \
    .format("delta") \
    .mode("overwrite") \
    .save("/opt/datalake/bronze/ecommerce_csv")

spark.stop()
