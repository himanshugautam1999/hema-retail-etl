CREATE DATABASE IF NOT EXISTS retail_sales;

-- ---------------------------------------------------------------------
-- Gold: Sales  (partitioned by order date)
-- ---------------------------------------------------------------------
CREATE EXTERNAL TABLE IF NOT EXISTS retail_sales.gold_sales (
    order_id        STRING,
    order_date      DATE,
    shipment_date   DATE,
    shipment_mode   STRING,
    city            STRING
)
PARTITIONED BY (year INT, month INT, day INT)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION 's3://hema-retail-sales/gold/sales/_symlink_format_manifest/';

-- After new partitions are written, we also need to make them visible via
--   MSCK REPAIR TABLE retail_sales.gold_sales;
-- (or ALTER TABLE ... ADD PARTITION for a targeted add.)

-- ---------------------------------------------------------------------
-- Gold: Customer  (unpartitioned dimension)
-- ---------------------------------------------------------------------
CREATE EXTERNAL TABLE IF NOT EXISTS retail_sales.gold_customer (
    customer_id            STRING,
    customer_first_name    STRING,
    customer_last_name     STRING,
    customer_segment       STRING,
    country                STRING,
    orders_last_month      BIGINT,
    orders_last_6_months   BIGINT,
    orders_all_time        BIGINT
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION 's3://hema-retail-sales/gold/customer/_symlink_format_manifest/';

