from pyspark.sql.functions import lower, trim, col, monotonically_increasing_id, regexp_replace, when,col, min, max, to_date, date_format,year,month, dayofmonth,weekofyear, quarter, dayofweek, sequence, explode, expr, row_number, from_json,sum as _sum, coalesce, try_to_date, lit, split

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType,DecimalType
from pyspark.sql.window import Window
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType
from pyspark.sql import Row
import builtins
import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BRONZE_DIR = os.path.join(BASE_DIR, "data", "bronze")
SILVER_DIR = os.path.join(BASE_DIR, "data", "silver")



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

def build_dim_product():
    spark = get_spark()          
    # Load bronze product codes
    products = spark.read.parquet("data/bronze/product_codes")

    # Clean columns
    dim_product = (
        products
        .withColumnRenamed("p_code", "product_code")
        .withColumnRenamed("p_name", "product_name")
        .withColumn("product_name", trim(col("product_name")))
        .withColumn("retail_price", regexp_replace(col("retail_price"), "[^0-9.]", "").cast(DoubleType()))
        .dropDuplicates(["product_code"])
    )

    # Add surrogate key
    dim_product = dim_product.withColumn(
        "product_key",
        monotonically_increasing_id()
    )

    # Reorder columns
    dim_product = dim_product.select(
        "product_key",
        "product_code",
        "product_name",
        "retail_price"
    )
    unknown_product = spark.createDataFrame(
    [(-1, "N/A", "Unknown Product", 0.0)],
    dim_product.schema
)
    dim_product = dim_product.unionByName(unknown_product)
    

    # Write to silver
    dim_product.write.mode("overwrite").parquet("data/silver/dim_product")

    print("dim_product created successfully")
    dim_product.printSchema()
    dim_product.show()



def build_dim_platform():
    spark = get_spark()          

    ad_spend = spark.read.parquet("data/bronze/daily_ad_spend")
    leads = spark.read.parquet("data/bronze/raw_web_leads")

    # Standardize ad_spend platform
    ad_platforms = (
        ad_spend
        .withColumn("platform_clean", lower(trim(col("platform"))))
        .withColumn(
            "platform_clean",
            when(col("platform_clean").isin("fb","facebook","facebã¶k" ), "facebook")
            .when(col("platform_clean").isin("g_ads","google_ads"), "google_ads")
            .when(col("platform_clean") == "tiktok", "tiktok")
            .otherwise(None)
        )
        .select(col("platform_clean").alias("platform"))
    )

    # Standardize raw_web_leads source
    lead_platforms = (
        leads
        .withColumn("platform_clean", lower(trim(col("source"))))
        .withColumn(
            "platform_clean",
            when(col("platform_clean").isin("fb", "fb_social", "facebook_ads"), "facebook")
            .when(col("platform_clean").isin("g_ads", "google_ads","google_paid","google-ads"), "google_ads")
            .when(col("platform_clean") == "organic", "organic")
            .otherwise("other")
        )
        .select(col("platform_clean").alias("platform"))
    )
    
    platforms = ad_platforms.union(lead_platforms).filter(col("platform").isNotNull()).distinct()


    window = Window.orderBy("platform")

    dim_platform = (
        platforms
        .withColumn("platform_key", row_number().over(window))
        .select("platform_key", "platform")
    )

    dim_platform.write.mode("overwrite").parquet("data/silver/dim_platform")

    print("dim_platform created successfully")
    spark.read.parquet("data/silver/dim_platform").show()
    ad_spend.select("platform").distinct().show(50, False)

    leads.select("source").distinct().show(50, False)

def build_dim_date():
    spark = get_spark()          

    # Load bronze tables
    sales = spark.read.parquet("data/bronze/sales_dump")
    ad_spend = spark.read.parquet("data/bronze/daily_ad_spend")
    leads = spark.read.parquet("data/bronze/raw_web_leads")
    interactions = spark.read.parquet("data/bronze/website_interactions")

    # Convert everything to DATE
    sales = sales.withColumn("sale_date", to_date(col("timestamp")))
    interactions = interactions.withColumn("interaction_date", to_date(col("ts")))
    ad_spend = ad_spend.withColumn("ad_date", expr("try_to_date(event_date, 'dd/MM/yyyy')"))
    leads = leads.withColumn("lead_date", expr("try_to_date(signup_date, 'dd/MM/yyyy')"))

    # Collect min & max from each
    min_dates = [
        sales.select(min("sale_date")).first()[0],
        interactions.select(min("interaction_date")).first()[0],
        ad_spend.select(min("ad_date")).first()[0],
        leads.select(min("lead_date")).first()[0],
    ]

    max_dates = [
        sales.select(max("sale_date")).first()[0],
        interactions.select(max("interaction_date")).first()[0],
        ad_spend.select(max("ad_date")).first()[0],
        leads.select(max("lead_date")).first()[0],
    ]

    global_min = builtins.min([d for d in min_dates if d is not None])
    global_max = builtins.max([d for d in max_dates if d is not None])

    # Generate full calendar
    date_df = spark.sql(f"""
        SELECT explode(sequence(
            to_date('{global_min}'),
            to_date('{global_max}'),
            interval 1 day
        )) AS full_date
    """)

    dim_date = (
        date_df
        .withColumn("date_key", date_format(col("full_date"), "yyyyMMdd").cast("int"))
        .withColumn("year", year(col("full_date")))
        .withColumn("month", month(col("full_date")))
        .withColumn("month_name", date_format(col("full_date"), "MMMM"))
        .withColumn("day", dayofmonth(col("full_date")))
        .withColumn("week_of_year", weekofyear(col("full_date")))
        .withColumn("quarter", quarter(col("full_date")))
        .withColumn("day_of_week", dayofweek(col("full_date")))
        .withColumn("is_weekend",
            when(dayofweek(col("full_date")).isin([1,7]), 1).otherwise(0)
        )
    )

    dim_date.write.mode("overwrite").parquet("data/silver/dim_date")

    print("dim_date created successfully")
    print(global_min, global_max)
    df = spark.read.parquet("data/silver/dim_date")
    print("Row count:", df.count())
    df.orderBy("full_date").show()
    df.orderBy(df.full_date.desc()).show(3)
    df.select("date_key", "full_date").show(5)
    df1=df.filter(col("full_date").isNull()).count()
    print(df1)
    df2=df.filter(col("date_key").isNull()).count()
    print(df2)
    df.select("full_date", "day_of_week", "is_weekend").show(10)

def build_dim_user():
    spark = get_spark()          
    # Load CRM users
    users = spark.read.parquet("data/bronze/crm_users_export")

    # Clean
    users = (
        users
        .withColumn("user_id", trim(col("user_id")))
        .withColumn("full_name", trim(col("full_name")))
        .withColumn("email", trim(col("email")))
        .withColumn("phone", trim(col("phone")))
        .withColumn("phone", regexp_replace(col("phone"), "[^0-9]", ""))
        .dropDuplicates(["user_id"])
    )

    # Create surrogate key
    window_spec = Window.partitionBy().orderBy("user_id")

    dim_user = users.withColumn(
        "user_key",
        row_number().over(window_spec)
    )

    # Reorder columns to match final structure
    dim_user = dim_user.select(
        "user_key",
        "user_id",
        "full_name",
        "email",
        "phone"
    )

    # Add UNKNOWN row safely using schema
    unknown_row = spark.createDataFrame(
        [(0, "UNKNOWN", "Unknown", None, None)],
        dim_user.schema
    )

    dim_user = dim_user.unionByName(unknown_row)

    dim_user.write.mode("overwrite").parquet("data/silver/dim_user")

    print("dim_user created successfully")
    spark.read.parquet("data/silver/dim_user").show()

def build_dim_influencer():
    spark = get_spark()          
    influencers = spark.read.parquet("data/bronze/influencer_campaigns")
    # influencers.printSchema()
    metadata_schema = StructType([
        StructField("handle", StringType(), True),
        StructField("reach", IntegerType(), True),
        StructField("rate", DoubleType(), True)
    ])

    influencers = influencers.withColumn(
        "metadata_parsed",
        from_json(col("metadata"), metadata_schema)
    )
    influencers = influencers.select(
        col("inf_id"),
        col("ref_code"),
        col("metadata_parsed.handle").alias("handle"),
        col("metadata_parsed.reach").alias("reach"),
        col("metadata_parsed.rate").alias("rate")
    )
    dim_influencer = (
        influencers
        .withColumn("handle", lower(trim(col("handle"))))
        .dropDuplicates(["inf_id"])
    )
    dim_influencer = dim_influencer.withColumn(
        "influencer_key",
        monotonically_increasing_id()
    )
    dim_influencer = dim_influencer.select(
        "influencer_key",
        "inf_id",
        "handle",
        "reach",
        "rate",
        "ref_code"
    )
    dim_influencer.write.mode("overwrite").parquet("data/silver/dim_influencer")
    print("dim_user created successfully")
    spark.read.parquet("data/silver/dim_influencer").show()
    

def build_fact_ad_spend():
    spark = get_spark()          
    ad_spend = spark.read.parquet("data/bronze/daily_ad_spend")
    dim_date = spark.read.parquet("data/silver/dim_date")
    dim_platform = spark.read.parquet("data/silver/dim_platform")

    # Clean platform names
    ad_spend = ad_spend.withColumn(
        "platform_clean",
        lower(trim(col("platform")))
    )

    ad_spend = ad_spend.withColumn(
    "event_date_clean",
    trim(col("event_date"))
    )

    ad_spend = ad_spend.withColumn(
        "platform_clean",
        when(col("platform_clean") == "tiktok", "tiktok")
        .when(col("platform_clean").isin("g_ads", "google_ads"), "google_ads")
        .when(col("platform_clean").isin("fb", "facebook", "facebã¶k"), "facebook")
        .otherwise(None)
    )
    ad_spend=ad_spend.filter(col("platform_clean").isNotNull())

    
    
    # Convert date
    ad_spend = ad_spend.withColumn(
    "event_date_clean",
    trim(col("event_date"))
)

    ad_spend = ad_spend.withColumn(
        "parsed_mdy",
        try_to_date(trim(col("event_date")), "M/d/yyyy")
    )

    # Step 2: parse day-first
    ad_spend = ad_spend.withColumn(
        "parsed_dmy",
        try_to_date(trim(col("event_date")), "dd/MM/yyyy")
    )

    # Step 3: choose correct one
    ad_spend = ad_spend.withColumn(
        "parsed_date",
        when(
            month(col("parsed_mdy")) <= 2,
            col("parsed_mdy")
        ).otherwise(
            col("parsed_dmy")
        )
    ).drop("parsed_mdy", "parsed_dmy")

    ad_spend = ad_spend.filter(col("parsed_date").isNotNull())

    ad_spend.select("event_date", "parsed_date").show(20, False)
    
    # ad_spend.filter(col("ad_date").isNull()).show()

#Aggregate (grain = platform per day)
    ad_spend_agg = (
    ad_spend
    .withColumn("cost", col("cost").cast("double"))
    .groupBy("parsed_date", "platform_clean")
    .agg(_sum("cost").alias("total_cost"))
    .withColumn("total_cost", col("total_cost").cast(DecimalType(10,2)))
)
    ad_spend_agg = ad_spend_agg.withColumn(
    "platform_clean",
    lower(trim(col("platform_clean")))
    )
    dim_platform = dim_platform.withColumn(
    "platform",
    lower(trim(col("platform")))
    )

    ad_spend_agg.join(
    dim_platform,
    ad_spend_agg.platform_clean == dim_platform.platform,
    "left"
    ).filter(col("platform_key").isNull()).select("platform_clean").distinct().show()
    #Join with dim_date
    ad_spend_joined = ad_spend_agg.join(
        dim_date,
        ad_spend_agg.parsed_date == dim_date.full_date,
        "left"
    )

    # Join with dim_platform
    ad_spend_joined = ad_spend_joined.join(
        dim_platform,
        ad_spend_joined.platform_clean == dim_platform.platform,
        "left"
    )

    # Final fact table
    fact_ad_spend = ad_spend_joined.select(
        col("date_key"),
        col("platform_key"),
        col("total_cost")
    )
# Write
    fact_ad_spend.write.mode("overwrite").parquet("data/silver/fact_ad_spend")
    # df=fact_ad_spend.filter(col("date_key").isNull()).count()
    # print("Date Key null valye:",df)
    print("fact_ad_spend created successfully")
    spark.read.parquet("data/silver/fact_ad_spend").show(68)
    df1=spark.read.parquet("data/silver/fact_ad_spend").count()
    print(df1)
    spark.read.parquet("data/silver/fact_ad_spend").filter(col("date_key").isNull()).show()
    dim_date.select("full_date").distinct().orderBy("full_date").show(100)
    ad_spend.select("event_date", "parsed_date").distinct().show(100, False)
    ad_spend_agg.select("platform_clean").distinct().show(50, False)

    dim_platform.select("platform").distinct().show(50, False)
    # ad_spend.printSchema()
    # dim_date.printSchema()
    ad_spend_joined.filter(col("platform_key").isNull()) \
    .select("platform_clean") \
    .distinct() \
    .show()


def build_fact_sales():
    spark = get_spark()          
    sales_df = spark.read.parquet("data/bronze/sales_dump")
    dim_date = spark.read.parquet("data/silver/dim_date")
    dim_user = spark.read.parquet("data/silver/dim_user")
    dim_product = spark.read.parquet("data/silver/dim_product")

    sales_df.select("amount").show(20, False)
    #Clean amount (remove $, spaces, commas, etc.)
    sales_clean = sales_df.withColumn(
    "currency",
    when(col("amount").contains("€"), "EUR")
    .when(col("amount").contains("$"), "USD")
    .otherwise("USD")
    )
    sales_clean = sales_clean.withColumn(
    "amount_numeric",
    regexp_replace(col("amount"), "[^0-9.-]", "").cast("double")
    )
    sales_clean = sales_clean.withColumn(
    "amount_dolor",
    when(col("amount_numeric") == -50, 0)
    .when(
        col("currency") == "EUR", 
        (col("amount_numeric") * lit(1.08)).cast(DecimalType(10, 2))
    )
    .otherwise(col("amount_numeric").cast(DecimalType(10, 2)))
)

    # Clean product_name (remove leading/trailing spaces)
    sales_clean = sales_clean.withColumn(
        "product_name",
        trim(col("product_name"))
    )

    sales_clean = sales_clean.withColumn(
    "date_key",
    date_format(col("timestamp"), "yyyyMMdd").cast("int")
)

    # Handle NULL and UNKNOWN user_id
    # sales_clean = sales_clean.fillna({"user_id":0})


    # Join with dim_user
    sales_joined = sales_clean.join(
        dim_user,
        "user_id",
        "left"
    )

    # Join with dim_product
    sales_joined = sales_joined.join(
        dim_product,
        "product_name",
        "left"
    )

    #final fact table
    fact_sales = sales_joined.select(
        col("sale_id"),
        col("date_key"),
        coalesce(col("user_key"), lit(0)).alias("user_key"),
        col("product_key"),
        col("amount_dolor").alias("amount")
    )

    fact_sales.write.mode("overwrite").parquet("data/silver/fact_sales")

    df=fact_sales.filter(col("date_key").isNull()).count()
    print(df)
    print("fact_sales created successfully")
    spark.read.parquet("data/silver/fact_sales").show(100)
    df1=fact_sales.filter(col("user_key").isNull()).count()
    print("User_key which are null",df1)
    df2=fact_sales.filter(col("product_key").isNull()).count()
    print("Product Keys are null",df2)
    sales_clean.agg(_sum("amount_numeric")).show()
    fact_sales.agg(_sum("amount")).show()
    sales_df.select("user_id").distinct().subtract(
    dim_user.select("user_id").distinct()
    ).show()

if __name__ == "__main__":
    build_dim_product()
    build_dim_platform()
    build_dim_date()
    build_dim_user()
    build_dim_influencer()
    build_fact_ad_spend()
    build_fact_sales()