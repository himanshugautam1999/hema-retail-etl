import sys

from awsglue.utils import getResolvedOptions
from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException

from common.logging_utils import get_logger
from common.spark_session import BRONZE_PATH, SILVER_PATH, build_spark

# PK which will be used for the upsertion part in silver layer
SILVER_KEY = "row_id"


def main():
    args = getResolvedOptions(sys.argv, ["key"])
    key = args["key"]

    log = get_logger("silver_job", {"layer": "silver", "key": key})
    spark = build_spark("hema-silver")

    # Read only the bronze rows that came from this file using the _source_file in bronze, 
    #so we can filter on it instead of rescanning all of bronze.   
    #Predicate pushdown basically
    bronze = spark.read.format("delta").load(BRONZE_PATH).filter(
        F.col("_source_file") == key
    )

    incoming_count = bronze.count()
    log.info(f"Bronze rows for this file: {incoming_count}")

    if incoming_count == 0:
        log.warning("No bronze rows found for this file; nothing to merge.")
        spark.stop()
        return

    # Perforing cleaning activity

    silver = (
        bronze.withColumn("row_id", F.col("row_id").cast("int"))
        .withColumn("order_id", F.trim(F.col("order_id")))
        .withColumn("order_date", F.to_date(F.col("order_date"), "M/d/yyyy"))
        .withColumn("ship_date", F.to_date(F.col("ship_date"), "M/d/yyyy"))
        .withColumn("ship_mode", F.trim(F.col("ship_mode")))
        .withColumn("customer_id", F.trim(F.col("customer_id")))
        .withColumn("segment", F.trim(F.col("segment")))
        .withColumn("country", F.trim(F.col("country")))
        .withColumn("city", F.trim(F.col("city")))
    )

    # split the Customer Name into first, last name as needed in gold layer
    # can also handles multi-word surnames

    silver = (
        silver.withColumn("customer_name", F.trim(F.col("customer_name")))
        .withColumn(
            "customer_first_name",
            F.split(F.col("customer_name"), " ", 2).getItem(0),
        )
        .withColumn(
            "customer_last_name",
            F.split(F.col("customer_name"), " ", 2).getItem(1),
        )
    )

    # Keeping the conformed columns we care about downstream.

    silver = silver.select(
        "row_id",
        "order_id",
        "order_date",
        "ship_date",
        "ship_mode",
        "customer_id",
        "customer_first_name",
        "customer_last_name",
        "segment",
        "country",
        "city",
        "year",
        "month",
        "day",
    )


    # Checking if the table already exists or not
    # If the table exists then do the merge operation else it's the first time run so create the table 

    try:
        target = DeltaTable.forPath(spark, SILVER_PATH)
        log.info("Silver table exists hence performing --> MERGE")
        (
            target.alias("t")
            .merge(silver.alias("s"), f"t.{SILVER_KEY} = s.{SILVER_KEY}")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    except AnalysisException:
        # First run: the table doesn't exist yet, so create it.
        log.info("Silver table does not exist yet --> creating it")
        (
            silver.write.format("delta")
            .mode("overwrite")
            .partitionBy("year", "month", "day")
            .option("mergeSchema", "true")
            .save(SILVER_PATH)
        )

    log.info(f"Silver MERGE complete for {incoming_count} incoming rows")
    spark.stop()


if __name__ == "__main__":
    main()
