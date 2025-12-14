import pandas as pd
import json
import os

# ---------------------------
# Path
# ---------------------------
CSV_PATH = "data/csv/ecomdataset.csv"
JSON_PATH = "data/json/sports_and_outdoors.jsonl"
GOLDEN_DIR = "datalake/golden"

os.makedirs(GOLDEN_DIR, exist_ok=True)

# ===========================
# 1️⃣ SALES SUMMARY (CSV)
# ===========================
csv_df = pd.read_csv(CSV_PATH, encoding="ISO-8859-1")

sales_summary = csv_df.groupby(
    ["StockCode", "Description"]
).agg(
    total_units_sold=("Quantity", "sum"),
    total_sales=("UnitPrice", lambda x: (x * csv_df.loc[x.index, "Quantity"]).sum())
).reset_index()

sales_summary.to_csv(
    f"{GOLDEN_DIR}/sales_summary.csv", index=False
)

print("✅ sales_summary.csv dibuat")

# ===========================
# 2️⃣ RATING SUMMARY (JSON)
# ===========================
chunks = []
MAX_ROWS = 1_000_000  # aman, streaming

with open(JSON_PATH, "r", encoding="utf-8") as f:
    for i, line in enumerate(f):
        if i >= MAX_ROWS:
            break
        chunks.append(json.loads(line))

json_df = pd.DataFrame(chunks)

rating_summary = json_df.groupby("asin").agg(
    avg_rating=("rating", "mean"),
    total_reviews=("rating", "count")
).reset_index()

rating_summary.to_csv(
    f"{GOLDEN_DIR}/rating_summary.csv", index=False
)

print("✅ rating_summary.csv dibuat")

# ===========================
# 3️⃣ PRODUK POPULER
# ===========================
popular_products = sales_summary.sort_values(
    by="total_units_sold", ascending=False
).head(50)

popular_products.to_csv(
    f"{GOLDEN_DIR}/popular_products.csv", index=False
)

# ===========================
# 4️⃣ PRODUK ULASAN NEGATIF
# ===========================
negative_products = rating_summary[
    rating_summary["avg_rating"] < 3
].sort_values(
    by="total_reviews", ascending=False
).head(50)

negative_products.to_csv(
    f"{GOLDEN_DIR}/negative_products.csv", index=False
)

print("🎉 GOLDEN LAYER SELESAI & RINGAN")
