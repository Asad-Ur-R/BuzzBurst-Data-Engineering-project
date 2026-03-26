import os
import time
import pandas as pd
from sqlalchemy import create_engine, text
from watchdog.observers import Observer
from pyspark.sql.functions import to_timestamp,coalesce as _coalesce, try_to_timestamp
from watchdog.events import FileSystemEventHandler
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lower, trim, when, regexp_replace, coalesce,
    date_format, lit, sum as _sum, round as _round
)
from pyspark.sql.types import DecimalType
from datetime import datetime

# Paths
BASE_DIR    = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
INCOMING    = os.path.join(BASE_DIR, "data", "incoming")
BRONZE_DIR  = os.path.join(BASE_DIR, "data", "bronze")
SILVER_DIR  = os.path.join(BASE_DIR, "data", "silver")
GOLD_DIR    = os.path.join(BASE_DIR, "data", "gold")

# Postgres Config
DB_USER     = "postgres"
DB_PASSWORD = "asad123"
DB_HOST     = "localhost"
DB_PORT     = "5432"
DB_NAME     = "buzzburst_gold"

# Supported Files
SUPPORTED_FILES = ["sales_dump.csv", "daily_ad_spend.csv"]

# HELPERS

def get_engine():
    url = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    return create_engine(url)


def get_spark():
    os.environ["PYSPARK_PYTHON"]        = r"C:\Program Files\Python311\python.exe"
    os.environ["PYSPARK_DRIVER_PYTHON"] = r"C:\Program Files\Python311\python.exe"
    return (
        SparkSession.builder
        .appName("BuzzBurst_Lambda")
        .master("local[*]")
        .getOrCreate()
    )


def upsert_to_postgres(df: pd.DataFrame, table: str, pk: str, engine):
    """
    MERGE logic using Postgres INSERT ON CONFLICT.
    - If PK already exists → UPDATE all columns
    - If PK is new        → INSERT new row
    This means we never duplicate, never lose data.
    """
    if df.empty:
        print(f"[LAMBDA] No rows to upsert into {table}")
        return

    cols        = df.columns.tolist()
    col_names   = ", ".join(cols)
    placeholders= ", ".join([f":{c}" for c in cols])
    updates     = ", ".join([f"{c} = EXCLUDED.{c}" for c in cols if c != pk])

    upsert_sql = text(f"""
        INSERT INTO {table} ({col_names})
        VALUES ({placeholders})
        ON CONFLICT ({pk})
        DO UPDATE SET {updates}
    """)

    with engine.begin() as conn:
        conn.execute(upsert_sql, df.to_dict(orient="records"))

    print(f"[LAMBDA] Upserted {len(df)} rows into {table}")


# PROCESS: sales_dump.csv

def process_sales(file_path: str, engine):
    file_path = file_path.replace("\\", "/")
    print(f"\n[LAMBDA] Processing sales file: {file_path}")
    spark = get_spark()

    # Load new file
    new_sales = (
        spark.read
        .option("header", True)
        .option("multiLine", True)
        .option("quote", '"')
        .option("escape", '"')
        .csv(file_path)
    )

    # Load existing fact_sales PKs from Postgres
    # We only process rows whose sale_id is NOT already in Postgres
    existing_ids = pd.read_sql("SELECT sale_id FROM fact_sales", engine)
    existing_set = set(existing_ids["sale_id"].tolist())

    # Load dims for joining
    dim_user    = spark.read.parquet(os.path.join(GOLD_DIR, "dim_user"))
    dim_product = spark.read.parquet(os.path.join(GOLD_DIR, "dim_product"))

    # Clean amount
    sales_clean = new_sales.withColumn(
        "currency",
        when(col("amount").contains("€"), "EUR").otherwise("USD")
    )
    sales_clean = sales_clean.withColumn(
        "amount_numeric",
        regexp_replace(col("amount"), "[^0-9.-]", "").cast("double")
    )
    sales_clean = sales_clean.withColumn(
        "amount_usd",
        when(col("amount_numeric") == -50, 0)
        .when(col("currency") == "EUR",
              (col("amount_numeric") * lit(1.08)).cast(DecimalType(10, 2)))
        .otherwise(col("amount_numeric").cast(DecimalType(10, 2)))
    )

    # Clean product_name
    sales_clean = sales_clean.withColumn(
        "product_name", trim(col("product_name"))
    )

    # date_key
    sales_clean = sales_clean.withColumn(
    "parsed_ts",
    _coalesce(
        # Primary format — matches your bronze data
        try_to_timestamp(col("timestamp")),
        # Fallback formats just in case
        try_to_timestamp(col("timestamp")),
        try_to_timestamp(col("timestamp")),
    )
)

    # Debug — print any rows where timestamp still couldn't be parsed
    unparsed = sales_clean.filter(col("parsed_ts").isNull())
    if unparsed.count() > 0:
        print("[LAMBDA] WARNING — unparseable timestamps:")
        unparsed.select("sale_id", "timestamp").show(5, False)

    sales_clean = sales_clean.withColumn(
        "date_key",
        date_format(col("parsed_ts"), "yyyyMMdd").cast("int")
    )

    # Drop rows where timestamp couldn't be parsed
    sales_clean = sales_clean.filter(col("date_key").isNotNull())

    # Join dims
    sales_joined = sales_clean.join(dim_user,    "user_id",      "left")
    sales_joined = sales_joined.join(dim_product, "product_name", "left")

    # Build fact_sales rows
    fact_new = sales_joined.select(
        col("sale_id"),
        col("date_key"),
        coalesce(col("user_key"),    lit(0)).alias("user_key"),
        coalesce(col("product_key"), lit(-1)).alias("product_key"),
        col("amount_usd").alias("amount")
    )

    # Filter only NEW sale_ids
    fact_pd = fact_new.toPandas()
    new_rows = fact_pd[~fact_pd["sale_id"].isin(existing_set)]

    print(f"[LAMBDA] Found {len(new_rows)} new sales rows")

    # Upsert fact_sales
    upsert_to_postgres(new_rows, "fact_sales", "sale_id", engine)

    # Recompute fact_marketing_performance
    refresh_marketing_performance(engine)


# PROCESS: daily_ad_spend.csv

def process_ad_spend(file_path: str, engine):
    file_path = file_path.replace("\\", "/")
    print(f"\n[LAMBDA] Processing ad spend file: {file_path}")
    spark = get_spark()

    # Load new file
    new_spend = (
        spark.read
        .option("header", True)
        .csv(file_path)
    )

    # Load dims
    dim_date     = spark.read.parquet(os.path.join(GOLD_DIR, "dim_date"))
    dim_platform = spark.read.parquet(os.path.join(GOLD_DIR, "dim_platform"))

    # Clean platform
    new_spend = new_spend.withColumn(
        "platform_clean",
        lower(trim(col("platform")))
    )
    new_spend = new_spend.withColumn(
        "platform_clean",
        when(col("platform_clean").isin("fb", "facebook", "facebã¶k"), "facebook")
        .when(col("platform_clean").isin("g_ads", "google_ads"), "google_ads")
        .when(col("platform_clean") == "tiktok", "tiktok")
        .otherwise(None)
    ).filter(col("platform_clean").isNotNull())

    # Parse date
    from pyspark.sql.functions import try_to_date, month
    new_spend = new_spend.withColumn(
        "parsed_mdy", try_to_date(trim(col("event_date")), "M/d/yyyy")
    )
    new_spend = new_spend.withColumn(
        "parsed_dmy", try_to_date(trim(col("event_date")), "dd/MM/yyyy")
    )
    new_spend = new_spend.withColumn(
        "parsed_date",
        when(month(col("parsed_mdy")) <= 2, col("parsed_mdy"))
        .otherwise(col("parsed_dmy"))
    ).filter(col("parsed_date").isNotNull())

    # Aggregate
    ad_agg = (
        new_spend
        .withColumn("cost", col("cost").cast("double"))
        .groupBy("parsed_date", "platform_clean")
        .agg(_sum("cost").alias("total_cost"))
        .withColumn("total_cost", col("total_cost").cast(DecimalType(10, 2)))
    )

    # Join dims
    ad_joined = ad_agg.join(
        dim_date, ad_agg.parsed_date == dim_date.full_date, "left"
    )
    ad_joined = ad_joined.join(
        dim_platform, ad_agg.platform_clean == dim_platform.platform, "left"
    )

    # Build fact rows
    from pyspark.sql.functions import monotonically_increasing_id
    fact_new = ad_joined.select(
        monotonically_increasing_id().alias("ad_spend_key"),
        col("date_key"),
        col("platform_key"),
        col("total_cost")
    ).filter(
        col("date_key").isNotNull() & col("platform_key").isNotNull()
    )

    fact_pd = fact_new.toPandas()
    print(f"[LAMBDA] Found {len(fact_pd)} ad spend rows")

    # Upsert fact_ad_spend
    upsert_to_postgres(fact_pd, "fact_ad_spend", "ad_spend_key", engine)

    # Recompute fact_marketing_performance
    refresh_marketing_performance(engine)


# REFRESH: fact_marketing_performance
# Called after EVERY upsert — keeps metrics live

def refresh_marketing_performance(engine):
    """
    Recomputes ROAS and CAC from latest Postgres data.
    Uses date_key as the natural conflict key — not perf_key.
    """
    print("[LAMBDA] Refreshing fact_marketing_performance...")

    sql = text("""
        WITH ad_by_date AS (
            SELECT
                date_key,
                SUM(total_cost::NUMERIC)        AS total_ad_spend
            FROM fact_ad_spend
            GROUP BY date_key
        ),
        sales_by_date AS (
            SELECT
                date_key,
                SUM(amount::NUMERIC)                            AS total_revenue,
                SUM(CASE WHEN user_key != 0 THEN 1 ELSE 0 END) AS paying_users
            FROM fact_sales
            GROUP BY date_key
        ),
        combined AS (
            SELECT
                COALESCE(a.date_key, s.date_key)    AS date_key,
                a.total_ad_spend,
                s.total_revenue,
                s.paying_users,
                CASE
                    WHEN a.total_ad_spend > 0
                    THEN ROUND(s.total_revenue / a.total_ad_spend, 4)
                    ELSE NULL
                END AS roas,
                CASE
                    WHEN s.paying_users > 0
                    THEN ROUND(a.total_ad_spend / s.paying_users, 2)
                    ELSE NULL
                END AS cac
            FROM ad_by_date a
            FULL OUTER JOIN sales_by_date s
            ON a.date_key = s.date_key
        )
        INSERT INTO fact_marketing_performance
            (date_key, total_ad_spend, total_revenue, paying_users, roas, cac)
        SELECT
            date_key, total_ad_spend, total_revenue, paying_users, roas, cac
        FROM combined
        ON CONFLICT (date_key)
        DO UPDATE SET
            total_ad_spend = EXCLUDED.total_ad_spend,
            total_revenue  = EXCLUDED.total_revenue,
            paying_users   = EXCLUDED.paying_users,
            roas           = EXCLUDED.roas,
            cac            = EXCLUDED.cac;
    """)

    with engine.begin() as conn:
        conn.execute(sql)

    print("[LAMBDA] fact_marketing_performance refreshed")


# WATCHDOG — File System Event Handler

class IncomingFileHandler(FileSystemEventHandler):

    def __init__(self):
        self.engine = get_engine()
        self.processing_delay = 2
        self.processed_files = set()  # ← prevent double processing

    def process_file(self, file_path):
        file_name = os.path.basename(file_path)

        # Skip if already processed in this session
        if file_path in self.processed_files:
            return

        if file_name not in SUPPORTED_FILES:
            print(f"[WATCHDOG] Ignored: {file_name}")
            return

        print(f"\n[WATCHDOG] Detected: {file_name} at {datetime.now()}")

        # Wait for file to finish writing
        time.sleep(self.processing_delay)

        try:
            self.processed_files.add(file_path)  # mark as processed

            if file_name == "sales_dump.csv":
                process_sales(file_path, self.engine)
            elif file_name == "daily_ad_spend.csv":
                process_ad_spend(file_path, self.engine)

            print(f"[WATCHDOG] Done processing: {file_name}")

        except Exception as e:
            print(f"[WATCHDOG] Error processing {file_name}: {e}")
            raise

    def on_created(self, event):
        if not event.is_directory:
            self.process_file(event.src_path)

    def on_modified(self, event):          # ← catches Windows copy behavior
        if not event.is_directory:
            self.process_file(event.src_path)


# ENTRY POINT

if __name__ == "__main__":
    print(f"[WATCHDOG] Starting — monitoring {INCOMING}")
    print(f"[WATCHDOG] Supported files: {SUPPORTED_FILES}")
    print(f"[WATCHDOG] Drop a CSV into data/incoming/ to trigger pipeline")
    print(f"[WATCHDOG] Press Ctrl+C to stop\n")

    handler  = IncomingFileHandler()
    observer = Observer()
    observer.schedule(handler, path=INCOMING, recursive=False)
    observer.start()

    try:
        while True:
            time.sleep(1)   # keep alive
    except KeyboardInterrupt:
        print("\n[WATCHDOG] Stopping...")
        observer.stop()

    observer.join()
    print("[WATCHDOG] Stopped")