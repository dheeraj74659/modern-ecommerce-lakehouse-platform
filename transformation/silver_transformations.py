import logging
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from config.pipeline_config import PIPELINE_CONFIG
from utils.spark_session import create_spark_session

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("SilverTransformation")

config = PIPELINE_CONFIG["silver"]

spark = create_spark_session("Silver-Incremental-Transform")


def main():
    try:
        bronze_df = spark.read.format("delta").load(config["source_path"])

        # Deduplicate using window function on primary key ordered by latest update
        window_spec = Window.partitionBy(config["primary_key"]) \
                            .orderBy(col(config["watermark_column"]).desc())

        deduped_df = bronze_df.withColumn(
            "row_num",
            row_number().over(window_spec)
        ).filter("row_num = 1").drop("row_num")

        # Business validations
        validated_df = deduped_df \
            .filter(col("order_amount") > 0) \
            .filter(col("customer_id").isNotNull())

        target_path = config["target_path"]

        if DeltaTable.isDeltaTable(spark, target_path):

            delta_table = DeltaTable.forPath(spark, target_path)

            # Upsert to maintain idempotency
            delta_table.alias("target").merge(
                validated_df.alias("source"),
                f"target.{config['primary_key']} = source.{config['primary_key']}"
            ).whenMatchedUpdateAll() \
             .whenNotMatchedInsertAll() \
             .execute()

        else:
            validated_df.write.format("delta") \
                .partitionBy(config["partition_column"]) \
                .save(target_path)

        logger.info("Silver layer transformation completed successfully.")

    except Exception as e:
        logger.error(f"Silver transformation failed: {str(e)}")
        raise


if __name__ == "__main__":
    main()
