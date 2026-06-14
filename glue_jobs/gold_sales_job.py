
import sys

from awsglue.utils import getResolvedOptions
from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException

from common.logging_utils import get_logger
from common.manifest_utils import enable_auto_manifest, generate_manifest
from common.spark_session import GOLD_SALES_PATH, SILVER_PATH, build_spark

#PK for this table to do the upsertion
GOLD_SALES_KEY = "order_id"


def main():
    args = getResolvedOptions(sys.argv, ["key"])
    key = args["key"]

    log = get_logger("gold_sales_job", {"layer": "gold_sales", "key": key})
    spark = build_spark("hema-gold-sales")

    # Reading the whole silver table
    silver_all = spark.read.format("delta").load(SILVER_PATH) 

    sales = (
        silver_all.select(
            F.col("order_id"),
            F.col("order_date"),
            F.col("ship_date").alias("shipment_date"),
            F.col("ship_mode").alias("shipment_mode"),
            F.col("city"),
            F.col("year"),
            F.col("month"),
            F.col("day"),
        )
        .dropDuplicates(["order_id"])
    )

    sales_count = sales.count()
    log.info(f"Computed {sales_count} order level sales rows")

    # Checking if the table already exists or not
    # If the table exists then do the merge operation else it's the first time run so create the table 

    try:
        target = DeltaTable.forPath(spark, GOLD_SALES_PATH)
        log.info("Gold Sales table exists; performing MERGE.")
        (
            target.alias("t")
            .merge(sales.alias("s"), f"t.{GOLD_SALES_KEY} = s.{GOLD_SALES_KEY}")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    except AnalysisException:
        log.info("Gold Sales table does not exist yet; creating it.")
        (
            sales.write.format("delta")
            .mode("overwrite")
            .partitionBy("year", "month", "day")
            .option("mergeSchema", "true")
            .save(GOLD_SALES_PATH)
        )

    log.info("Gold Sales MERGE complete.")

    # Most Important -> how do we query this delta table in athena? Using symlink manifest
    # Keep the symlink manifest in sync so the Athena external table reads the
    # current version. enable_auto_manifest makes Delta regenerate it on future
    # writes, generate_manifest guarantees one exists right now (incl. first run).
    enable_auto_manifest(spark, GOLD_SALES_PATH)
    generate_manifest(spark, GOLD_SALES_PATH)

    spark.stop()


if __name__ == "__main__":
    main()
