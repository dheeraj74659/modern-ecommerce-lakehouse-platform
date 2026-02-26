import logging
from pyspark.sql.functions import col, current_timestamp, max as spark_max
from delta.tables import DeltaTable
from config.pipeline_config import PIPELINE_CONFIG
from utils.spark_session import create_spark_session

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("BronzeLoader")

config = PIPELINE_CONFIG["bronze"]

spark = create_spark_session("Bronze-Incremental-Load")

def get_last_processed_watermark():
    """
    Fetches last processed watermark from metadata table.
    If table doesn't exist, returns None (initial load).
    """
    try:
        watermark_df = spark.read.table(config["checkpoint_table"])
        return watermark_df.select(
            spark_max("last_watermark")
        ).collect()[0][0]
    except Exception:
        logger.info("No watermark found. Running full initial load.")
        return None


def update_watermark(new_watermark):
    """
    Updates metadata table with new watermark after successful load.
    """
    watermark_df = spark.createDataFrame(
        [(new_watermark,)],
        ["last_watermark"]
    )
    watermark_df.write.mode("overwrite").saveAsTable(config["checkpoint_table"])


def main():
    try:
        last_watermark = get_last_processed_watermark()

        df = spark.read.format("csv") \
            .option("header", True) \
            .load(config["source_path"])

        # Convert watermark column to timestamp
        df = df.withColumn(config["watermark_column"], col(config["watermark_column"]).cast("timestamp"))

        if last_watermark:
            # Incremental filtering
            df = df.filter(col(config["watermark_column"]) > last_watermark)

        if df.isEmpty():
            logger.info("No new data to process.")
            return

        df = df.withColumn("ingestion_timestamp", current_timestamp())

        target_path = config["target_path"]

        if DeltaTable.isDeltaTable(spark, target_path):

            delta_table = DeltaTable.forPath(spark, target_path)

            # Idempotent upsert using MERGE
            delta_table.alias("target").merge(
                df.alias("source"),
                f"target.{config['primary_key']} = source.{config['primary_key']}"
            ).whenMatchedUpdateAll() \
             .whenNotMatchedInsertAll() \
             .execute()

        else:
            df.write.format("delta") \
                .partitionBy(config["partition_column"]) \
                .save(target_path)

        # Update watermark after successful commit
        new_watermark = df.agg(
            spark_max(config["watermark_column"])
        ).collect()[0][0]

        update_watermark(new_watermark)

        logger.info("Bronze layer load completed successfully.")

    except Exception as e:
        logger.error(f"Bronze load failed: {str(e)}")
        raise


if __name__ == "__main__":
    main()
