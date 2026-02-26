import logging
from pyspark.sql.functions import col, sum as spark_sum
from delta.tables import DeltaTable
from utils.spark_session import create_spark_session
from data_quality.validation_framework import DataQualityValidator

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("FactOrders")

spark = create_spark_session("Gold-Fact-Orders")

SILVER_ORDERS = "s3://ecommerce-lakehouse/silver/orders/"
DIM_CUSTOMERS = "s3://ecommerce-lakehouse/gold/dim_customers/"
DIM_PRODUCTS = "s3://ecommerce-lakehouse/gold/dim_products/"
GOLD_PATH = "s3://ecommerce-lakehouse/gold/fact_orders/"

def main():
    try:
        orders_df = spark.read.format("delta").load(SILVER_ORDERS)
        dim_customers = spark.read.format("delta").load(DIM_CUSTOMERS) \
            .filter("is_current = true")
        dim_products = spark.read.format("delta").load(DIM_PRODUCTS)

        # ---------------------------------------------------
        # Resolve surrogate keys
        # ---------------------------------------------------
        fact_df = orders_df \
            .join(dim_customers, "customer_id") \
            .join(dim_products, "product_id") \
            .groupBy(
                "order_id",
                "customer_sk",
                "product_sk",
                "order_date"
            ).agg(
                spark_sum("order_amount").alias("total_amount")
            )
        
        # Run validations before writing
        validator = DataQualityValidator()
        validator.validate_not_null(fact_df, ["order_id", "customer_sk", "product_sk"])
        validator.validate_no_duplicates(fact_df, "order_id")
        validator.validate_foreign_key(fact_df, dim_customers, "customer_sk", "customer_sk")
        
        validator.validate()
        
        if not DeltaTable.isDeltaTable(spark, GOLD_PATH):

            fact_df.write.format("delta") \
                .partitionBy("order_date") \
                .save(GOLD_PATH)
            return

        delta_table = DeltaTable.forPath(spark, GOLD_PATH)
        
        # Idempotent upsert for fact table
        delta_table.alias("target").merge(
            fact_df.alias("source"),
            "target.order_id = source.order_id"
        ).whenMatchedUpdateAll() \
         .whenNotMatchedInsertAll() \
         .execute()

        logger.info("Fact table updated successfully.")

    except Exception as e:
        logger.error(f"Fact load failed: {str(e)}")
        raise


if __name__ == "__main__":
    main()
