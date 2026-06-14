
import sys

from awsglue.utils import getResolvedOptions
from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.utils import AnalysisException

from common.logging_utils import get_logger
from common.manifest_utils import enable_auto_manifest, generate_manifest
from common.spark_session import (
    DATASET_LATEST_DAY,
    GOLD_CUSTOMER_PATH,
    SILVER_PATH,
    build_spark,
)

#PK for this table to do the upsertion - Customer level grain
GOLD_CUSTOMER_KEY = "customer_id"


def main():
    args = getResolvedOptions(sys.argv, ["key"])
    key = args["key"]

    log = get_logger("gold_customer_job", {"layer": "gold_customer", "key": key})
    spark = build_spark("hema-gold-customer")

    silver_all = spark.read.format("delta").load(SILVER_PATH)

    # Window boundaries, anchored to the fixed dataset latest day as given

    anchor = F.to_date(F.lit(DATASET_LATEST_DAY))
    last_month_start = F.add_months(anchor, -1)
    last_6_months_start = F.add_months(anchor, -6)

    log.info(
        f"Computing customer metrics anchored to {DATASET_LATEST_DAY} "
        f"(last_month from add_months(-1), last_6m from add_months(-6)) "
    )

    # Per customer: distinct order counts over each window
    # compute on distinct (customer_id, order_id, order_date) so multiple
    # line items of the same order count once.
    orders = silver_all.select(
        "customer_id", "order_id", "order_date"
    ).dropDuplicates(["customer_id", "order_id"]) #imp to drop duplicates , customer can buy multiple item but each item will have same order number


    metrics = orders.groupBy("customer_id").agg(
        F.countDistinct(
            F.when(
                (F.col("order_date") > last_month_start)
                & (F.col("order_date") <= anchor),
                F.col("order_id"),
            )
        ).alias("orders_last_month"),
        F.countDistinct(
            F.when(
                (F.col("order_date") > last_6_months_start)
                & (F.col("order_date") <= anchor),
                F.col("order_id"),
            )
        ).alias("orders_last_6_months"),
        F.countDistinct(F.col("order_id")).alias("orders_all_time"),
    )

    # Attach the descriptive attributes, a customer's name/segment/country can
    # in principle vary across rows. we take the most recent by order_date.
    attrs_window = silver_all.select(
        "customer_id",
        "customer_first_name",
        "customer_last_name",
        "segment",
        "country",
        "order_date",
    )
    latest_attrs = (
        attrs_window.withColumn(
            "rn",
            F.row_number().over(
                Window.partitionBy("customer_id").orderBy(F.col("order_date").desc())
            ),
        )
        .filter(F.col("rn") == 1)
        .drop("rn", "order_date")
    )

    customer = (
        latest_attrs.join(metrics, on="customer_id", how="inner")
        .select(
            F.col("customer_id"),
            F.col("customer_first_name"),
            F.col("customer_last_name"),
            F.col("segment").alias("customer_segment"),
            F.col("country"),
            F.col("orders_last_month"),
            F.col("orders_last_6_months"),
            F.col("orders_all_time"),
        )
    )

    cust_count = customer.count()
    log.info(f"Computed metrics for {cust_count} customers.")


    # Checking if the table already exists or not
    # If the table exists then do the merge operation else it's the first time run so create the table 

    try:
        target = DeltaTable.forPath(spark, GOLD_CUSTOMER_PATH)
        log.info("Gold Customer table exists; performing MERGE.")
        (
            target.alias("t")
            .merge(
                customer.alias("s"),
                f"t.{GOLD_CUSTOMER_KEY} = s.{GOLD_CUSTOMER_KEY}",
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    except AnalysisException:
        log.info("Gold Customer table does not exist yet; creating it.")
        (
            customer.write.format("delta")
            .mode("overwrite")
            .option("mergeSchema", "true")
            .save(GOLD_CUSTOMER_PATH)
        )

    log.info("Gold Customer MERGE complete.")

    # Keep the symlink manifest in sync for the Athena external table. Very Important Step
    
    enable_auto_manifest(spark, GOLD_CUSTOMER_PATH)
    generate_manifest(spark, GOLD_CUSTOMER_PATH)

    spark.stop()


if __name__ == "__main__":
    main()
