import logging
from pyspark.sql.functions import col, monotonically_increasing_id
from delta.tables import DeltaTable
from utils.spark_session import create_spark_session

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("DimProduct")

spark = create_spark_session("Gold-Dim-Product")

SILVER_PATH = "s3://ecommerce-lakehouse/silver/products/"
GOLD_PATH = "s3://ecommerce-lakehouse/gold/dim_products/"

def main():
    try:
        source_df = spark.read.format("delta").load(SILVER_PATH)

        if not DeltaTable.isDeltaTable(spark, GOLD_PATH):

            dim_df = source_df \
                .withColumn("product_sk", monotonically_increasing_id())

            dim_df.write.format("delta").save(GOLD_PATH)
            return

        delta_table = DeltaTable.forPath(spark, GOLD_PATH)

        delta_table.alias("target").merge(
            source_df.alias("source"),
            "target.product_id = source.product_id"
        ).whenNotMatchedInsert(
            values={
                "product_id": "source.product_id",
                "product_name": "source.product_name",
                "category": "source.category",
                "price": "source.price"
            }
        ).execute()

        logger.info("Product dimension updated successfully.")

    except Exception as e:
        logger.error(f"Product dimension failed: {str(e)}")
        raise


if __name__ == "__main__":
    main()
