CREATE TABLE IF NOT EXISTS delta.`abfss://bronze@adfdatabricks456.dfs.core.windows.net/_control/ingestion_watermarks/` (
    source_system STRING COMMENT 'Source identifier',
    schema_name STRING COMMENT 'Source schema name',
    table_name STRING COMMENT 'Source table name',
    primary_key_columns STRING COMMENT 'Comma-separated list of primary key column names from the source table',
    watermark_column STRING COMMENT 'Name of the column used for incremental extraction',
    last_watermark_value TIMESTAMP COMMENT 'The timestamp of the last successful data extraction for incremental loads',
    load_frequency STRING COMMENT 'Defines how often the table is processed',
    is_active BOOLEAN COMMENT 'Flag to enable or disable processing for a specific table',
    last_run_timestamp TIMESTAMP COMMENT 'Timestamp when the pipeline last completed processing this table',
    last_run_status STRING COMMENT 'Status of the last pipeline run for this table (SUCCESS, FAILED, RUNNING)',
    rows_extracted BIGINT COMMENT 'Number of rows extracted during the last successful run',
    batch_id STRING COMMENT 'The ADF pipeline run ID for traceability',
    created_date TIMESTAMP COMMENT 'Timestamp when this control table record was created',
    updated_date TIMESTAMP COMMENT 'Timestamp when this control table record was last updated'
)
USING DELTA
LOCATION 'abfss://bronze@adfdatabricks456.dfs.core.windows.net/_control/ingestion_watermarks/'
TBLPROPERTIES (
    'delta.logRetentionDuration' = 'interval 30 days',
    'delta.deletedFileRetentionDuration' = 'interval 7 days'
);

-- Seed initial data for known source tables idempotently
INSERT INTO delta.`abfss://bronze@adfdatabricks456.dfs.core.windows.net/_control/ingestion_watermarks/`
(source_system, schema_name, table_name, primary_key_columns, watermark_column, last_watermark_value, load_frequency, is_active, last_run_timestamp, last_run_status, rows_extracted, batch_id, created_date, updated_date)
SELECT
    new_data.source_system,
    new_data.schema_name,
    new_data.table_name,
    new_data.primary_key_columns,
    new_data.watermark_column,
    new_data.last_watermark_value,
    new_data.load_frequency,
    new_data.is_active,
    new_data.last_run_timestamp,
    new_data.last_run_status,
    new_data.rows_extracted,
    new_data.batch_id,
    new_data.created_date,
    new_data.updated_date
FROM VALUES
    ('ERP', 'Sales', 'customers', 'CustomerID', 'LastModifiedDate', CAST('1900-01-01 00:00:00.000' AS TIMESTAMP), 'Daily Batch', TRUE, NULL, 'PENDING', 0, NULL, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
    ('ERP', 'Sales', 'orders', 'OrderID', 'OrderDate', CAST('1900-01-01 00:00:00.000' AS TIMESTAMP), 'Daily Batch', TRUE, NULL, 'PENDING', 0, NULL, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
    ('CRM', 'Marketing', 'leads', 'LeadID', 'CreatedDate', CAST('1900-01-01 00:00:00.000' AS TIMESTAMP), 'Near Realtime', TRUE, NULL, 'PENDING', 0, NULL, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
    ('CRM', 'Inventory', 'products', 'ProductID', NULL, CAST('1900-01-01 00:00:00.000' AS TIMESTAMP), 'Quarterly', TRUE, NULL, 'PENDING', 0, NULL, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP())
AS new_data(source_system, schema_name, table_name, primary_key_columns, watermark_column, last_watermark_value, load_frequency, is_active, last_run_timestamp, last_run_status, rows_extracted, batch_id, created_date, updated_date)
LEFT ANTI JOIN delta.`abfss://bronze@adfdatabricks456.dfs.core.windows.net/_control/ingestion_watermarks/` existing_data
    ON new_data.source_system = existing_data.source_system
    AND new_data.schema_name = existing_data.schema_name
    AND new_data.table_name = existing_data.table_name;