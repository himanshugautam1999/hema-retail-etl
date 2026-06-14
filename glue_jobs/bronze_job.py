
import sys
from datetime import datetime, timezone

from awsglue.utils import getResolvedOptions
from pyspark.sql import functions as F

from common.logging_utils import get_logger
from common.spark_session import BRONZE_PATH, build_spark


def main():
    args = getResolvedOptions(sys.argv, ["bucket", "key"])
    bucket, key = args["bucket"], args["key"]
    source_uri = f"s3://{bucket}/{key}"

    log = get_logger("bronze_job", {"layer": "bronze", "key": key})
    log.info(f"Reading a single source file: {source_uri}")

    spark = build_spark("hema-bronze")

    # Reading the file now
    # Also I have not infered the schema and have kept it in string to follow the principles of medallion architecture
    raw = (
        spark.read.option("header", "true")
        .option("inferSchema", "false")  # <---
        .csv(source_uri)
    )

    row_count = raw.count()
    log.info(f"Source file row count: {row_count}")

    # Performing the normalization to maintain sanity --> "Order ID" -> "order_id"
    for old in raw.columns:
        raw = raw.withColumnRenamed(old, old.strip().lower().replace(" ", "_"))

    ingested_at = datetime.now(timezone.utc).isoformat()

    # Order Date in the Superstore dataset is M/D/YYYY. Parse it to derive
    # partition columns. We keep the original string column intact too.
    bronze = (
        raw.withColumn("_source_file", F.lit(key))
        .withColumn("_ingested_at", F.lit(ingested_at))
        .withColumn("_order_date_parsed", F.to_date(F.col("order_date"), "M/d/yyyy"))
        .withColumn("year", F.year("_order_date_parsed"))
        .withColumn("month", F.month("_order_date_parsed"))
        .withColumn("day", F.dayofmonth("_order_date_parsed"))
    )

    # Append into the bronze Delta table, partitioned by date.  (We never delete/update the records from bronze layer)
    # mergeSchema=true means if a future file adds a new column, the table
    # absorbs it instead of failing and directly serving handle schema changes dynamically
    (
        bronze.write.format("delta")
        .mode("append")
        .partitionBy("year", "month", "day")
        .option("mergeSchema", "true")
        .save(BRONZE_PATH)
    )

    log.info(f"Appended {row_count} rows to bronze Delta table at {BRONZE_PATH}")
    spark.stop()


if __name__ == "__main__":
    main()
