from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Join CSV and JSON to Silver - Safe") \
    .getOrCreate()

# =====================
# READ BRONZE DELTA
# =====================
csv_df = spark.read.format("delta").load("/opt/datalake/bronze/ecommerce_csv")
json_df = spark.read.format("delta").load("/opt/datalake/bronze/sports_and_outdoors")

# =====================
# TRANSFORM & SAMPLE
# =====================
csv_df = csv_df.select("InvoiceNo", "StockCode", "Description", "Quantity", "UnitPrice", "Country")
# json_df = json_df.select("review_id", "product_id", "rating", "review_text")
json_df = json_df.select("user_id", "asin", "rating", "text")
# 🔹 AMAN: pakai limit untuk testing / dataset besar
# csv_df = csv_df.limit(50000)   # ambil 50000 baris pertama
# json_df = json_df.limit(50000)  # ambil 50000 baris pertama

# =====================
# JOIN LOGIC
# =====================
# Misal csv_df.StockCode → json_df.asin
joined_df = csv_df.join(json_df, csv_df.StockCode == json_df.asin, "inner")


# =====================
# WRITE SILVER DELTA
# =====================
joined_df.write.format("delta") \
    .mode("overwrite") \
    .save("/opt/datalake/silver/ecommerce_reviews")

spark.stop()
