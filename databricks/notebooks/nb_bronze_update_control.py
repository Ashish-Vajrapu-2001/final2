# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook: nb_bronze_update_control
# MAGIC
# MAGIC ## Description
# MAGIC This notebook is responsible for updating the bronze ingestion control table (`bronze_control_table`) with the latest ingestion status, watermark value, and timestamps for a specific source table. It is typically called as a downstream process after a bronze ingestion notebook completes, either successfully or with a failure.
# MAGIC
# MAGIC ## Specifications
# MAGIC - Language: Python (PySpark)
# MAGIC - Dependencies: Azure Data Lake Storage Gen2 (ADLS Gen2) for Delta Lake control table.
# MAGIC - Input Parameters: Widgets for source system, schema, table, status, new watermark, rows extracted, and batch ID.
# MAGIC - Output: Confirmation message via `dbutils.notebook.exit()`.
# MAGIC
# MAGIC ## Logic Steps
# MAGIC 1.  Define and retrieve notebook widgets for parameters.
# MAGIC 2.  Construct the full path to the bronze control table.
# MAGIC 3.  Load the bronze control table as a Delta DataFrame.
# MAGIC 4.  Prepare the update statement to modify `LastWatermarkValue` (if successful), `IngestionStatus`, and `LastIngestionTimestamp` based on the input parameters.
# MAGIC 5.  Execute a `MERGE INTO` operation to atomically update the control table.
# MAGIC 6.  Implement error handling and log the outcome.
# MAGIC 7.  Exit the notebook with a status message.

# COMMAND ----------
# MAGIC %md
# MAGIC ### 1. Define and Retrieve Notebook Widgets
# MAGIC
# MAGIC These widgets are used to pass parameters to the notebook, typically from Azure Data Factory or another orchestrator.

# COMMAND ----------

dbutils.widgets.text("p_source_system_code", "", "Source System Code")
dbutils.widgets.text("p_schema_name", "", "Source Schema Name")
dbutils.widgets.text("p_table_name", "", "Source Table Name")
dbutils.widgets.text("p_status", "FAILED", "Ingestion Status (SUCCESS/FAILED)")
dbutils.widgets.text("p_new_watermark", "1900-01-01 00:00:00.000", "New Watermark Value")
dbutils.widgets.text("p_rows_extracted", "0", "Number of Rows Extracted")
dbutils.widgets.text("p_batch_id", "", "Pipeline Run ID (Batch ID)")

# COMMAND ----------

# Retrieve widget values
source_system_code = dbutils.widgets.get("p_source_system_code")
schema_name = dbutils.widgets.get("p_schema_name")
table_name = dbutils.widgets.get("p_table_name")
status = dbutils.widgets.get("p_status").upper() # Ensure uppercase
new_watermark_value = dbutils.widgets.get("p_new_watermark")
rows_extracted = int(dbutils.widgets.get("p_rows_extracted"))
batch_id = dbutils.widgets.get("p_batch_id")

print(f"Parameters received:")
print(f"  Source System Code: {source_system_code}")
print(f"  Schema Name: {schema_name}")
print(f"  Table Name: {table_name}")
print(f"  Status: {status}")
print(f"  New Watermark Value: {new_watermark_value}")
print(f"  Rows Extracted: {rows_extracted}")
print(f"  Batch ID: {batch_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Configuration and Control Table Path
# MAGIC
# MAGIC Define the path to the bronze control table using the provided context.

# COMMAND ----------

# Control table path from condensed context
BRONZE_CONTROL_TABLE_PATH = "abfss://bronze@adlsbronzedev.dfs.core.windows.net/control/bronze_control"
BRONZE_CONTROL_TABLE_NAME = "bronze_control_table" # Assuming this name for clarity in SQL operations

print(f"Bronze control table path: {BRONZE_CONTROL_TABLE_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Update Control Table Logic
# MAGIC
# MAGIC This section constructs and executes a `MERGE INTO` statement to update the control table.
# MAGIC - If the ingestion `status` is 'SUCCESS', the `LastWatermarkValue` is updated to `p_new_watermark`.
# MAGIC - The `IngestionStatus` is always updated to the provided `p_status`.
# MAGIC - `LastIngestionTimestamp` is updated to the current UTC timestamp.

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

try:
    print(f"Attempting to update control table for: {source_system_code}.{schema_name}.{table_name}")

    # Create a temporary view for the control table if it doesn't exist as a global table
    spark.sql(f"CREATE TABLE IF NOT EXISTS default.{BRONZE_CONTROL_TABLE_NAME} USING DELTA LOCATION '{BRONZE_CONTROL_TABLE_PATH}'")

    # Determine the watermark to update. If FAILED, we should not update the watermark value.
    # The artifact description says "or the old watermark value if failed".
    # Assuming `new_watermark_value` passed in already reflects this logic from the orchestrator.
    # However, to be safe, if status is FAILED, we explicitly revert to the current watermark.
    
    # Read the current state of the control table to get the existing watermark if needed for failure scenario
    control_df = spark.read.format("delta").load(BRONZE_CONTROL_TABLE_PATH)
    current_watermark_df = control_df.filter(
        (control_df.SourceSystemCode == source_system_code) &
        (control_df.SchemaName == schema_name) &
        (control_df.TableName == table_name)
    ).select("LastWatermarkValue").collect()
    
    current_existing_watermark = current_watermark_df[0]["LastWatermarkValue"] if current_watermark_df else "1900-01-01 00:00:00.000"

    # The watermark to be used in the update statement
    watermark_to_set = new_watermark_value if status == "SUCCESS" else current_existing_watermark

    # SQL MERGE statement
    # The control table schema doesn't include _rows_extracted or _batch_id,
    # so we update only the defined columns from the context.
    merge_sql = f"""
    MERGE INTO default.{BRONZE_CONTROL_TABLE_NAME} AS target
    USING (
      SELECT
        '{source_system_code}' AS SourceSystemCode,
        '{schema_name}' AS SchemaName,
        '{table_name}' AS TableName,
        '{watermark_to_set}' AS NewLastWatermarkValue,
        '{status}' AS NewIngestionStatus,
        current_timestamp() AS NewLastIngestionTimestamp
    ) AS source
    ON target.SourceSystemCode = source.SourceSystemCode
       AND target.SchemaName = source.SchemaName
       AND target.TableName = source.TableName
    WHEN MATCHED THEN
      UPDATE SET
        target.LastWatermarkValue = source.NewLastWatermarkValue,
        target.IngestionStatus = source.NewIngestionStatus,
        target.LastIngestionTimestamp = source.NewLastIngestionTimestamp
    WHEN NOT MATCHED THEN
      INSERT (SourceSystemCode, SchemaName, TableName, LastWatermarkValue, LoadType, IngestionStatus, LastIngestionTimestamp, IsActive)
      VALUES (source.SourceSystemCode, source.SchemaName, source.TableName, source.NewLastWatermarkValue, 'INITIAL', source.NewIngestionStatus, source.NewLastIngestionTimestamp, TRUE);
    """
    
    print("Executing MERGE statement:")
    print(merge_sql)

    spark.sql(merge_sql)

    message = f"Successfully updated control table for {source_system_code}.{schema_name}.{table_name} with status '{status}' and watermark '{watermark_to_set}'. Rows extracted: {rows_extracted}, Batch ID: {batch_id}."
    print(message)
    dbutils.notebook.exit(message)

except Exception as e:
    error_message = f"Failed to update control table for {source_system_code}.{schema_name}.{table_name}. Error: {str(e)}"
    print(error_message)
    dbutils.notebook.exit(error_message)
