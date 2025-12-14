from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum

spark = SparkSession.builder \
    .appName("Silver to Golden") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

sales_df = spark.read.format("delta").load("/opt/datalake/silver/ecommerce_reviews")

gold_sales = sales_df.groupBy("StockCode", "Description") \
    .agg(
        _sum(col("Quantity") * col("UnitPrice")).alias("total_sales"),
        _sum("Quantity").alias("total_units_sold")
    )

gold_sales.write.format("delta") \
    .mode("overwrite") \
    .save("/opt/datalake/golden/sales_summary")

gold_ratings = sales_df.groupBy("StockCode", "Description") \
    .agg(  
        _sum(col("Quantity") * col("UnitPrice")).alias("total_sales"),
        _sum("Quantity").alias("total_units_sold")
    )

gold_ratings.write.format("delta") \
    .mode("overwrite") \
    .save("/opt/datalake/golden/ratings_summary")

gold_negative_reviews = sales_df.filter(col("rating") < 3)

gold_negative_reviews.write.format("delta") \
    .mode("overwrite") \
    .save("/opt/datalake/golden/negative_reviews")

gold_popular_products = sales_df.filter(col("total_units_sold") > 100)

gold_popular_products.write.format("delta") \
    .mode("overwrite") \
    .save("/opt/datalake/golden/popular_products")

spark.stop()
