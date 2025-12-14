import pandas as pd
from sqlalchemy import create_engine

engine = create_engine(
    "postgresql+psycopg2://admin:admin@localhost:5432/ecommerce"

)

files = {
    "sales_summary": "datalake/golden/sales_summary",
    "rating_summary": "datalake/golden/rating_summary",
    "popular_products": "datalake/golden/popular_products",
    "negative_products": "datalake/golden/negative_products"
}

for table, path in files.items():
    df = pd.read_csv(path)
    df.to_sql(table, engine, if_exists="replace", index=False)
    print(f"✅ {table} loaded")

df.to_sql(
    table,
    engine,
    if_exists="replace",
    index=False,
    chunksize=10000,
    method="multi"
)
