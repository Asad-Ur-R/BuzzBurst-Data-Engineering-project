import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os

#Paths
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
GOLD_DIR = os.path.join(BASE_DIR, "data", "gold")

# Connection Config
load_dotenv()  # reads your .env file

DB_USER     = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST     = os.getenv("DB_HOST")
DB_PORT     = os.getenv("DB_PORT")
DB_NAME     = os.getenv("DB_NAME")

#Tables → Primary Keys
# PKs are stored here for the Lambda upsert logic later
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

def get_engine():
    url = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    return create_engine(url)


def load_gold_to_postgres():
    """
    Reads each Gold parquet table and loads it into Postgres.
    Uses if_exists='replace' for the initial full load.
    Lambda layer will use upsert logic instead.
    """
    engine = get_engine()

    #Test connection first
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        print("[POSTGRES] Connection successful")
    except Exception as e:
        print(f"[POSTGRES] Connection failed: {e}")
        raise

    # Load each table
    for table_name, pk in TABLES.items():

        parquet_path = os.path.join(GOLD_DIR, table_name)

        # Check parquet exists before reading
        if not os.path.exists(parquet_path):
            print(f"[SKIP] {table_name} — parquet not found at {parquet_path}")
            continue

        # Read parquet into pandas
        df = pd.read_parquet(parquet_path)

        # Convert Decimal columns to float
        # Postgres can handle Decimal but pandas sometimes writes them as objects which breaks sqlalchemy
        for col in df.columns:
            if str(df[col].dtype) == "object":
                try:
                    df[col] = pd.to_numeric(df[col], errors="ignore")
                except:
                    pass

        # Write to Postgres — replace on full load
        df.to_sql(
            name=table_name,
            con=engine,
            if_exists="replace",   # Lambda layer will use upsert instead
            index=False,
            method="multi",        # bulk insert, faster than row by row
            chunksize=1000
        )

        print(f"[POSTGRES] {table_name:35s} → {len(df):>6} rows loaded")

    print("\n[POSTGRES] All Gold tables loaded successfully")


def verify_load():
    """
    Quick row count check after loading.
    Run this to confirm everything landed correctly.
    """
    engine = get_engine()

    print("\n── Verification ─────────────────────────────────────")
    with engine.connect() as conn:
        for table_name in TABLES:
            result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
            count = result.fetchone()[0]
            print(f"  {table_name:35s} → {count:>6} rows")


if __name__ == "__main__":
    load_gold_to_postgres()
    verify_load()