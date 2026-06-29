# Databricks PySpark Pipeline for IoT Manufacturing Data
# This script is designed to be run in a Databricks Notebook.

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, max, min, count, when, window

# Initialize Spark session (Required for local testing, Databricks already provides 'spark' variable)
try:
    # If running in Databricks, 'spark' is already defined
    spark
    print("Running in Databricks environment")
except NameError:
    print("Running locally, initializing SparkSession")
    spark = SparkSession.builder \
        .appName("Manufacturing_IoT_Pipeline") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .getOrCreate()

# --- Configurations ---
# Adjust these paths depending on where the data is located.
# In Databricks, this might be 'dbfs:/FileStore/tables/data/raw/...'
BASE_DIR = os.path.dirname(os.path.abspath(__file__)) if '__file__' in globals() else "."
RAW_DATA_PATH = f"file:///{BASE_DIR}/data/raw/assembly_line_iot_data.csv".replace("\\", "/")
GOLD_DATA_PATH = f"file:///{BASE_DIR}/data/gold/kpi_metrics".replace("\\", "/")

def run_pipeline():
    print(f"Reading raw data from: {RAW_DATA_PATH}")
    
    # --- BRONZE LAYER: Ingestion ---
    df_bronze = spark.read.csv(RAW_DATA_PATH, header=True, inferSchema=True)
    
    # --- SILVER LAYER: Cleansing and Formatting ---
    # In a real scenario, handle nulls and incorrect formats here
    df_silver = df_bronze.dropna()
    
    # Cast timestamp
    df_silver = df_silver.withColumn("timestamp", col("timestamp").cast("timestamp"))
    
    # --- GOLD LAYER: Aggregations for Power BI (Using Spark SQL) ---
    print("Aggregating KPIs for Gold layer using Spark SQL...")
    
    # Create a temporary view to use standard SQL
    df_silver.createOrReplaceTempView("silver_iot_data")
    
    # Execute SQL query to calculate KPIs and anomaly counts simultaneously
    sql_query = """
        SELECT 
            machine_id,
            AVG(temperature_c) AS avg_temperature,
            MAX(temperature_c) AS max_temperature,
            AVG(vibration_hz) AS avg_vibration,
            AVG(cycle_time_sec) AS avg_cycle_time,
            SUM(CASE WHEN status = 'Anomaly' THEN 1 ELSE 0 END) AS anomaly_count
        FROM silver_iot_data
        GROUP BY machine_id
    """
    df_gold_final = spark.sql(sql_query)
    
    # Output to console to verify
    df_gold_final.show()
    
    # Save the Gold data as a single CSV for easy Power BI import in this local project
    # In production Databricks, this would be saved as a Delta table (e.g., format("delta").saveAsTable("gold_iot_metrics"))
    print(f"Writing gold data to: {GOLD_DATA_PATH}")
    
    # To save as a single CSV locally (coalesce to 1 partition)
    df_gold_final.coalesce(1).write.mode("overwrite").csv(GOLD_DATA_PATH, header=True)
    print("Pipeline execution complete. Ready for Power BI.")

if __name__ == "__main__":
    run_pipeline()
