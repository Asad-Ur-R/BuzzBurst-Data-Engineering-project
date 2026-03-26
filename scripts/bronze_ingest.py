import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name


# CONFIG
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SOURCE_DIR = os.path.join(BASE_DIR, "data", "cloud_source")
BRONZE_DIR = os.path.join(BASE_DIR, "data", "bronze")

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

def ingest_all():
    """
    Ingests all CSVs from cloud_source into Bronze as Parquet.
    Called by Prefect as a single Bronze task.
    """

    spark = get_spark()

    for file in os.listdir(SOURCE_DIR):
        if file.endswith(".csv"):
            print(f"Ingesting: {file}")

            file_path = os.path.join(SOURCE_DIR, file)

            df = (
                spark.read
                .option("header", True)
                .option("multiLine", True)
                .option("quote", '"')
                .option("escape", '"')
                .option("mode", "PERMISSIVE")
                .csv(file_path)
            )

            df = df.withColumn("ingestion_timestamp", current_timestamp())
            df = df.withColumn("source_file_name", input_file_name())

            table_name = file.replace(".csv", "")
            target_path = os.path.join(BRONZE_DIR, table_name)

            df.write.mode("overwrite").parquet(target_path)

            print(f"Saved: {target_path} | Rows: {df.count()}")

    print("Bronze ingestion completed.")

if __name__ == "__main__":
    ingest_all()