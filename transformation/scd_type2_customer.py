import logging
from pyspark.sql.functions import col, current_date, lit, sha2, concat_ws
from delta.tables import DeltaTable
from utils.spark_session import create_spark_session

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("SCDType2Customer")

spark = create_spark_session("Customer-SCD2")

SILVER_PATH = "s3://ecommerce-lakehouse/silver/customers/"
GOLD_PATH = "s3://ecommerce-lakehouse/gold/dim_customers/"

def main():
    try:
        source_df = spark.read.format("delta").load(SILVER_PATH)

        # -------------------------------------------------------
        # Create hash of tracked columns to detect change
        # This avoids column-by-column comparison in merge
        # -------------------------------------------------------
        tracked_columns = ["name", "email", "address", "loyalty_tier"]

        source_df = source_df.withColumn(
            "record_hash",
            sha2(concat_ws("||", *tracked_columns), 256)
        )

        if not DeltaTable.isDeltaTable(spark, GOLD_PATH):

            logger.info("Initial load of customer dimension.")

            initial_df = source_df \
                .withColumn("start_date", current_date()) \
                .withColumn("end_date", lit(None).cast("date")) \
                .withColumn("is_current", lit(True))

            initial_df.write.format("delta").save(GOLD_PATH)
            return

        delta_table = DeltaTable.forPath(spark, GOLD_PATH)

        # -------------------------------------------------------
        # Step 1: Expire changed records
        # -------------------------------------------------------
        delta_table.alias("target").merge(
            source_df.alias("source"),
            """
            target.customer_id = source.customer_id
            AND target.is_current = true
            """
        ).whenMatchedUpdate(
            condition="target.record_hash <> source.record_hash",
            set={
                "end_date": "current_date()",
                "is_current": "false"
            }
        ).execute()

        # -------------------------------------------------------
        # Step 2: Insert new records
        # -------------------------------------------------------
        delta_table.alias("target").merge(
            source_df.alias("source"),
            """
            target.customer_id = source.customer_id
            AND target.record_hash = source.record_hash
            AND target.is_current = true
            """
        ).whenNotMatchedInsert(
            values={
                "customer_id": "source.customer_id",
                "name": "source.name",
                "email": "source.email",
                "address": "source.address",
                "loyalty_tier": "source.loyalty_tier",
                "record_hash": "source.record_hash",
                "start_date": "current_date()",
                "end_date": "null",
                "is_current": "true"
            }
        ).execute()

        logger.info("SCD Type 2 processing completed successfully.")

    except Exception as e:
        logger.error(f"SCD2 process failed: {str(e)}")
        raise


if __name__ == "__main__":
    main()
