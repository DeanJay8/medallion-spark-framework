from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, current_timestamp, sum, count

def run_bronze(spark: SparkSession, source_path: str, target_table: str, checkpoint: str):
    """Ingests raw JSON data using Auto Loader."""
    return (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", checkpoint)
        .load(source_path)
        .writeStream
        .option("checkpointLocation", checkpoint)
        .trigger(availableNow=True)
        .toTable(target_table))

def run_silver(spark, source_table, target_table, lookup_df):
    # 1. Read Bronze and cast the ID immediately
    # We use .alias() to make the references crystal clear
    df_bronze = spark.read.table(source_table).alias("bronze")
    df_lookup = lookup_df.alias("lookup")
    
    # 2. Perform the join by explicitly casting the join key
    # This prevents the [CANNOT_RESOLVE_DATAFRAME_COLUMN] error
    silver_df = df_bronze.join(
        df_lookup, 
        df_bronze["source_id"].cast("long") == df_lookup["source_id"], 
        how="left"
    )
    
    # 3. Clean up the columns (dropping the duplicate source_id from lookup)
    # And specifically selecting the partner_name we need
    final_df = silver_df.select(
        "bronze.*", 
        col("lookup.partner_name")
    ).fillna({"partner_name": "Unmapped_Partner"})
    
    # 4. Save with overwriteSchema
    (final_df.write 
     .mode("overwrite") 
     .option("overwriteSchema", "true") 
     .saveAsTable(target_table))
    
    print(f"✅ Silver table {target_table} updated and column references resolved.")

def run_gold(spark, source_table, target_table, group_cols, agg_map):
    """Aggregates data and automatically cleanses illegal column characters."""
    df = spark.read.table(source_table)
    summary_df = df.groupBy(group_cols).agg(agg_map)
    
    # Clean up column names like 'sum(amount)' to 'sum_amount'
    for c in summary_df.columns:
        clean_name = c.replace("(", "_").replace(")", "").replace(" ", "_")
        summary_df = summary_df.withColumnRenamed(c, clean_name)
    
    summary_df.write.mode("overwrite").saveAsTable(target_table)