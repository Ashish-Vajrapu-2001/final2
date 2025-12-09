INSERT INTO delta.`abfss://bronze@adlsbronzedev.dfs.core.windows.net/control/bronze_control`
(
  SourceSystemCode,
  SchemaName,
  TableName,
  LastWatermarkValue,
  LoadType,
  IngestionStatus,
  LastIngestionTimestamp,
  IsActive
)
VALUES
  ('ERP', 'Sales', 'customers', '1900-01-01 00:00:00.000', 'INITIAL', 'PENDING', NULL, TRUE),
  ('ERP', 'Sales', 'orders', '1900-01-01 00:00:00.000', 'INITIAL', 'PENDING', NULL, TRUE),
  ('CRM', 'Marketing', 'leads', '1900-01-01 00:00:00.000', 'INITIAL', 'PENDING', NULL, TRUE),
  ('CRM', 'Inventory', 'products', '1900-01-01 00:00:00.000', 'INITIAL', 'PENDING', NULL, TRUE);