
from pyspark.sql import SparkSession

# --- S3 paths for each medallion layer ----

BRONZE_PATH = "s3://hema-retail-sales/bronze/orders"
SILVER_PATH = "s3://hema-retail-sales/silver/orders"
GOLD_SALES_PATH = "s3://hema-retail-sales/gold/sales"
GOLD_CUSTOMER_PATH = "s3://hema-retail-sales/gold/customer"

# Glue Data Catalog database the Delta tables register under
CATALOG_DB = "retail_sales"

# The dataset's fixed latest day
DATASET_LATEST_DAY = "2018-12-30"


def build_spark(app_name: str) -> SparkSession:
    """
    Build a SparkSession with Delta Lake enabled using the below config to enable merge, schema evolution
    """
    return (
        SparkSession.builder.appName(app_name)
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )
