import os
import pandas as pd
from sqlalchemy import create_engine, text

NEON_URL = "postgresql://neondb_owner:npg_zYojqdQH13Jb@ep-tiny-sunset-an28ee2u-pooler.c-6.us-east-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require"

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
GOLD_DIR = os.path.join(BASE_DIR, "data", "gold")

TABLES = {
    "dim_user"                   : "user_key",
    "dim_product"                : "product_key",
    "dim_platform"               : "platform_key",
    "dim_date"                   : "date_key",
    "dim_influencer"             : "influencer_key",
    "fact_ad_spend"              : "ad_spend_key",
    "fact_sales"                 : "sale_id",
    "fact_marketing_performance" : "perf_key",
}

def load_to_neon():
    engine = create_engine(NEON_URL)

    # Test connection
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))
    print("[NEON] Connection successful")

    for table_name in TABLES:
        parquet_path = os.path.join(GOLD_DIR, table_name)

        if not os.path.exists(parquet_path):
            print(f"[SKIP] {table_name} not found")
            continue

        df = pd.read_parquet(parquet_path)

        # Convert decimals to float for Neon compatibility
        for col in df.columns:
            if str(df[col].dtype) == "object":
                try:
                    df[col] = pd.to_numeric(df[col], errors="ignore")
                except:
                    pass

        df.to_sql(
            name=table_name,
            con=engine,
            if_exists="replace",
            index=False,
            method="multi",
            chunksize=500
        )
        print(f"[NEON] {table_name:35s} → {len(df):>6} rows")

    print("\n[NEON] All tables loaded successfully")

if __name__ == "__main__":
    load_to_neon()