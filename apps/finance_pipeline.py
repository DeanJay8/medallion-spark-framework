from pyspark.sql import SparkSession
from src.config import *
from src.engine import run_bronze, run_silver, run_gold

# Initialize Spark
spark = SparkSession.builder.getOrCreate()

# 1. Ingest
print("Starting Bronze Layer...")
run_bronze(spark, f"{BASE_PATH}finance/raw/", TABLE_BRONZE, CHECKPOINT_BRONZE)

# 2. Transform (with a mock lookup for the demo)
print("Starting Silver Layer...")
partners = spark.createDataFrame([(101, "BMO_Partner"), (102, "Air_Miles_Vendor")], ["source_id", "name"])
run_silver(spark, TABLE_BRONZE, TABLE_SILVER, CHECKPOINT_SILVER, lookup_df=partners)

# 3. Aggregate
print("Starting Gold Layer...")
run_gold(spark, TABLE_SILVER, TABLE_GOLD, ["name"], {"amount": "sum"})

print("Pipeline Complete. Final Gold Data:")
spark.read.table(TABLE_GOLD).show()