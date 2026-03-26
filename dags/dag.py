import sys
import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(BASE_DIR, "scripts"))
from gold_to_postgres import load_gold_to_postgres

from prefect import flow, task, get_run_logger
from prefect.task_runners import ThreadPoolTaskRunner
from datetime import datetime

from bronze_ingest import ingest_all
from silver_transform import (
    build_dim_product, build_dim_platform, build_dim_date,
    build_dim_user, build_dim_influencer,
    build_fact_ad_spend, build_fact_sales
)
from gold import (
    build_gold_dimensions, build_gold_fact_ad_spend,
    build_gold_fact_sales, build_fact_marketing_performance
)


# Bronze

@task(name="Bronze: Ingest All Sources", log_prints=True)
def task_bronze():
    ingest_all()


# SILVER

@task(name="Silver: dim_product",log_prints=True)
def task_silver_product():      
    build_dim_product()

@task(name="Silver: dim_platform",log_prints=True)
def task_silver_platform():     
    build_dim_platform()

@task(name="Silver: dim_date",log_prints=True)
def task_silver_date():         
    build_dim_date()

@task(name="Silver: dim_user",log_prints=True)
def task_silver_user():         
    build_dim_user()

@task(name="Silver: dim_influencer",log_prints=True)
def task_silver_influencer():   
    build_dim_influencer()

@task(name="Silver: fact_ad_spend",log_prints=True)
def task_silver_fact_ad_spend(): 
    build_fact_ad_spend()

@task(name="Silver: fact_sales",log_prints=True)
def task_silver_fact_sales():   
    build_fact_sales()


# GOLD
@task(name="Gold: Promote Dimensions",log_prints=True)
def task_gold_dims():           
    build_gold_dimensions()

@task(name="Gold: fact_ad_spend",log_prints=True)
def task_gold_fact_ad_spend():  
    build_gold_fact_ad_spend()

@task(name="Gold: fact_sales",log_prints=True)
def task_gold_fact_sales():     
    build_gold_fact_sales()

@task(name="Gold: fact_marketing_performance", log_prints=True)
def task_gold_marketing():      
    build_fact_marketing_performance()

# Gold to postgres

@task(name="Gold → Postgres: Load All Tables", log_prints=True)
def task_load_postgres():
    load_gold_to_postgres()

# MAIN FLOW — Sequential to avoid Spark thread conflicts

@flow(
    name="BuzzBurst Medallion Pipeline",
    description="Bronze → Silver → Gold | BuzzBurst Media",
    task_runner=ThreadPoolTaskRunner(max_workers=1)
)
def buzzburst_pipeline():
    logger = get_run_logger()
    logger.info(f"Pipeline started at {datetime.now()}")

    #BRONZE
    logger.info("Starting Bronze layer...")
    task_bronze()

    #SILVER Dims
    # Sequential: each dim waits for the previous one
    logger.info("Starting Silver dimensions...")
    task_silver_product()
    task_silver_platform()
    task_silver_date()
    task_silver_user()
    task_silver_influencer()

    #SILVER FACTS
    # Facts run after ALL dims are done
    logger.info("Starting Silver facts...")
    task_silver_fact_ad_spend()
    task_silver_fact_sales()

    #GOLD DIMS
    logger.info("Starting Gold layer...")
    task_gold_dims()

    #GOLD FACTS
    task_gold_fact_ad_spend()
    task_gold_fact_sales()

    #CENTRAL FACT
    task_gold_marketing()
    task_load_postgres()

    logger.info(f"Pipeline completed successfully at {datetime.now()}")


if __name__ == "__main__":
    buzzburst_pipeline()