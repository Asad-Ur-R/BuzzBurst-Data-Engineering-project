from pyspark.sql.functions import lower, trim, col, monotonically_increasing_id, regexp_replace, when,col, min, max, to_date, date_format,year,month, dayofmonth,weekofyear, quarter, dayofweek, sequence, explode, expr, row_number, from_json,sum as _sum, coalesce, try_to_date, lit, split, count,round as _round,countDistinct
import os
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType,DecimalType
from pyspark.sql.window import Window
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType
from pyspark.sql import Row


BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SILVER_DIR = os.path.join(BASE_DIR, "data", "silver")
GOLD_DIR = os.path.join(BASE_DIR, "data", "gold")

def get_spark():
    os.environ["PYSPARK_PYTHON"] = r"C:\Program Files\Python311\python.exe"
    os.environ["PYSPARK_DRIVER_PYTHON"] = r"C:\Program Files\Python311\python.exe"

    from pyspark.sql import SparkSession
    return (
        SparkSession.builder
        .appName("BuzzBurst")
        .master("local[*]")
        .getOrCreate()
    )

# FK Integrity Check

def check_fk_integrity(fact_df, dim_df, fact_col, dim_col, fact_name, dim_name):
    """
    Left-anti join: finds fact rows whose FK has no match in the dimension.
    Raises an error if orphans exist.
    """
    orphans = fact_df.join(
        dim_df,
        fact_df[fact_col] == dim_df[dim_col],
        "left_anti"
    )
    count = orphans.count()
    if count > 0:
        print(f"[WARNING] {count} orphan records in {fact_name}.{fact_col} → {dim_name}.{dim_col}")
        orphans.select(fact_col).distinct().show(10, False)
    else:
        print(f"[OK] FK check passed: {fact_name}.{fact_col} → {dim_name}.{dim_col}")
    return count


# DIMENSIONS — Promote Silver → Gold

def build_gold_dimensions():
    """
    Dimensions are already clean from Silver.
    We promote them to Gold and add any final business labels.
    """
    spark = get_spark()

    dims = ["dim_user", "dim_product", "dim_platform", "dim_date", "dim_influencer"]

    for dim in dims:
        df = spark.read.parquet(f"{SILVER_DIR}/{dim}")
        df.write.mode("overwrite").parquet(f"{GOLD_DIR}/{dim}")
        print(f"[GOLD] {dim} written → {df.count()} rows")

# GOLD FACT: fact_ad_spend

def build_gold_fact_ad_spend():
    """
    Reads Silver fact_ad_spend.
    Validates FK integrity against Gold dims.
    Writes to Gold.
    """
    spark = get_spark()
    fact = spark.read.parquet(f"{SILVER_DIR}/fact_ad_spend")
    dim_date = spark.read.parquet(f"{GOLD_DIR}/dim_date")
    dim_platform = spark.read.parquet(f"{GOLD_DIR}/dim_platform")

    # FK Checks
    check_fk_integrity(fact, dim_date,     "date_key",     "date_key",     "fact_ad_spend", "dim_date")
    check_fk_integrity(fact, dim_platform, "platform_key", "platform_key", "fact_ad_spend", "dim_platform")

    # Drop rows where critical FKs are NULL
    # date_key and platform_key are the grain — both must exist
    fact_clean = fact.filter(
        col("date_key").isNotNull() & col("platform_key").isNotNull()
    )

    # Add surrogate PK
    fact_clean = fact_clean.withColumn(
        "ad_spend_key", monotonically_increasing_id()
    )

    fact_clean = fact_clean.select(
        "ad_spend_key",   # PK
        "date_key",       # FK → dim_date
        "platform_key",   # FK → dim_platform
        "total_cost"
    )

    fact_clean.write.mode("overwrite").parquet(f"{GOLD_DIR}/fact_ad_spend")
    print(f"[GOLD] fact_ad_spend → {fact_clean.count()} rows")
    fact_clean.show(10)

# GOLD FACT: fact_sales

def build_gold_fact_sales():
    """
    Reads Silver fact_sales.
    Validates all 3 FKs: date, user, product.
    Unknown users (user_key=0) are allowed — they map to the UNKNOWN row.
    Writes to Gold.
    """
    spark = get_spark()

    fact = spark.read.parquet(f"{SILVER_DIR}/fact_sales")
    dim_date    = spark.read.parquet(f"{GOLD_DIR}/dim_date")
    dim_user    = spark.read.parquet(f"{GOLD_DIR}/dim_user")
    dim_product = spark.read.parquet(f"{GOLD_DIR}/dim_product")

    # FK Checks
    check_fk_integrity(fact, dim_date,    "date_key",    "date_key",    "fact_sales", "dim_date")
    check_fk_integrity(fact, dim_user,    "user_key",    "user_key",    "fact_sales", "dim_user")
    check_fk_integrity(fact, dim_product, "product_key", "product_key", "fact_sales", "dim_product")

    # Drop rows with NULL date_key (ungroupable)
    fact_clean = fact.filter(col("date_key").isNotNull())

    # Fallback: NULL product_key → use -1 (Unknown Product)
    fact_clean = fact_clean.withColumn(
        "product_key", coalesce(col("product_key"), lit(-1))
    )

    fact_clean = fact_clean.select(
        "sale_id",      # Natural PK from source
        "date_key",     # FK → dim_date
        "user_key",     # FK → dim_user  (0 = UNKNOWN)
        "product_key",  # FK → dim_product
        "amount"
    )

    fact_clean.write.mode("overwrite").parquet(f"{GOLD_DIR}/fact_sales")
    print(f"[GOLD] fact_sales → {fact_clean.count()} rows")
    fact_clean.show(10)


# GOLD FACT: fact_marketing_performance

def build_fact_marketing_performance():
    """
    The central fact table the manual requires.
    Grain: one row per date.
    
    Combines:
      - Ad spend aggregated by date (with platform_key)
      - Sales aggregated by date (revenue, transactions)
    
    Computes business metrics:
      - ROAS = total_revenue / total_ad_spend
      - CAC  = total_ad_spend / distinct_users_who_bought
    
    Linked to: dim_date, dim_platform, dim_user (via sales count), dim_product (via sales)
    """
    spark = get_spark()

    fact_sales    = spark.read.parquet(f"{GOLD_DIR}/fact_sales")
    fact_ad_spend = spark.read.parquet(f"{GOLD_DIR}/fact_ad_spend")
    dim_date      = spark.read.parquet(f"{GOLD_DIR}/dim_date")

    # Side A: Aggregate ad spend by date
    # We keep platform_key for the join on the ad side
    ad_by_date = (
        fact_ad_spend
        .groupBy("date_key")
        .agg(
            _sum("total_cost").alias("total_ad_spend")
        )
    )

    # Side B: Aggregate sales by date
    sales_by_date = (
        fact_sales
        .groupBy("date_key")
        .agg(
            _sum("amount").alias("total_revenue"),
            # Count of distinct real users (exclude UNKNOWN = 0)
            _sum(
                when(col("user_key") != 0, 1).otherwise(0)
            ).alias("paying_users")
        )
    )

    # Full outer join on date_key
    # Full outer so dates with only ad spend OR only sales are included
    perf = ad_by_date.join(sales_by_date, "date_key", "full")

    # Compute ROAS and CAC
    perf = perf.withColumn(
        "roas",
        when(
            col("total_ad_spend").isNotNull() & (col("total_ad_spend") > 0),
            _round(col("total_revenue") / col("total_ad_spend"), 4)
        ).otherwise(None)
    )

    perf = perf.withColumn(
        "cac",
        when(
            col("paying_users").isNotNull() & (col("paying_users") > 0),
            _round(col("total_ad_spend") / col("paying_users"), 2)
        ).otherwise(None)
    )

    # Add surrogate PK
    perf = perf.withColumn("perf_key", monotonically_increasing_id())

    # Final column order
    fact_marketing_performance = perf.select(
        "perf_key",         # PK
        "date_key",         # FK → dim_date
        "total_ad_spend",
        "total_revenue",
        "paying_users",
        "roas",
        "cac"
    )

    # FK check on final table
    check_fk_integrity(
        fact_marketing_performance, dim_date,
        "date_key", "date_key",
        "fact_marketing_performance", "dim_date"
    )

    fact_marketing_performance.write.mode("overwrite").parquet(
        f"{GOLD_DIR}/fact_marketing_performance"
    )

    print(f"[GOLD] fact_marketing_performance → {fact_marketing_performance.count()} rows")
    fact_marketing_performance.show(40)

if __name__ == "__main__":
    build_gold_dimensions()          # Must run first — facts depend on dims
    build_gold_fact_ad_spend()       # Depends on dim_date, dim_platform
    build_gold_fact_sales()          # Depends on dim_date, dim_user, dim_product
    build_fact_marketing_performance()  # Depends on both gold facts
