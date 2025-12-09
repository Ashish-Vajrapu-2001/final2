# COMMAND ----------
# DBTITLE 1,Imports
from pyspark.sql import DataFrame
from pyspark.sql.functions import current_timestamp, lit, to_date
from datetime import datetime
import json

# COMMAND ----------
# DBTITLE 1,Widget Definitions
dbutils.widgets.text("p_source_system", "", "Identifier for the source system (e.g., 'crm', 'erp').")
dbutils.widgets.text("p_schema_name", "", "The schema name of the source table.")
dbutils.widgets.text("p_table_name", "", "The name of the source table.")
dbutils.widgets.text("p_staging_path", "", "Full ADLS path to the temporary staged Parquet files.")
dbutils.widgets.text("p_bronze_path", "", "Full ADLS path to the target Bronze Delta Lake table.")
dbutils.widgets.text("p_write_mode", "append", "The write mode for Delta Lake ('append' or 'overwrite').")
dbutils.widgets.text("p_batch_id", "", "The pipeline run ID for traceability.")

# COMMAND ----------
# DBTITLE 1,Retrieve Parameters
source_system = dbutils.widgets.get("p_source_system")
schema_name = dbutils.widgets.get("p_schema_name")
table_name = dbutils.widgets.get("p_table_name")
staging_path = dbutils.widgets.get("p_staging_path")
bronze_path = dbutils.widgets.get("p_bronze_path")
write_mode = dbutils.widgets.get("p_write_mode").lower() # Ensure lowercase for consistency
batch_id = dbutils.widgets.get("p_batch_id")

# Log parameters for debugging and traceability
print(f"--- Notebook Parameters ---")
print(f"Source System Code: {source_system}")
print(f"Schema Name: {schema_name}")
print(f"Table Name: {table_name}")
print(f"Staging Path: {staging_path}")
print(f"Bronze Path: {bronze_path}")
print(f"Write Mode: {write_mode}")
print(f"Batch ID: {batch_id}")
print(f"---------------------------")

# COMMAND ----------
# DBTITLE 1,Main Ingestion Logic
rows_processed = 0
error_message = None

try:
    print(f"Starting bronze ingestion for table: {source_system}.{schema_name}.{table_name}")
    print(f"Reading data from: {staging_path}")

    # Step 1: Read Parquet from staging path into DataFrame.
    df_staged = spark.read.parquet(staging_path)
    initial_staged_row_count = df_staged.count()
    print(f"Successfully read {initial_staged_row_count} records from staging path.")

    # Step 2: Add metadata columns to the DataFrame.
    # Metadata columns defined in CONDENSED_CONTEXT and ARTIFACT_TO_GENERATE specs.
    df_bronze = df_staged.withColumn("_bronze_ingestion_timestamp", current_timestamp()) \
                         .withColumn("_ingestion_date", to_date(current_timestamp())) \
                         .withColumn("_source_file_path", lit(staging_path)) \
                         .withColumn("_source_system_code", lit(source_system)) \
                         .withColumn("_batch_id", lit(batch_id))

    print("Added standard metadata columns to the DataFrame:")
    df_bronze.printSchema()

    # Step 3: Write DataFrame to Bronze Delta path with mergeSchema=true and partitioned by _ingestion_date.
    print(f"Writing data to Bronze Delta Lake at: {bronze_path}")
    print(f"Using write mode: '{write_mode}' and mergeSchema=true.")

    df_bronze.write \
             .format("delta") \
             .mode(write_mode) \
             .option("mergeSchema", "true") \
             .partitionBy("_ingestion_date") \
             .save(bronze_path)

    # Step 4: Capture row count.
    rows_processed = df_bronze.count()
    print(f"Successfully wrote {rows_processed} records to Bronze Delta Lake.")

except Exception as e:
    error_message = f"Bronze ingestion failed: {e}"
    print(error_message)
    # On error, exit immediately with error details and 0 rows processed.
    dbutils.notebook.exit(json.dumps({"rows_processed": 0, "error": error_message}))

# COMMAND ----------
# DBTITLE 1,Return Value
# If the try block completes successfully, exit with the processed row count.
output_json = {
    "rows_processed": rows_processed
}

dbutils.notebook.exit(json.dumps(output_json))