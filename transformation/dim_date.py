from pyspark.sql.functions import col, year, month, dayofmonth, quarter
from utils.spark_session import create_spark_session

spark = create_spark_session("Gold-Dim-Date")

GOLD_PATH = "s3://ecommerce-lakehouse/gold/dim_date/"

orders_df = spark.read.format("delta").load(
    "s3://ecommerce-lakehouse/silver/orders/"
)

date_df = orders_df.select(col("order_date")).distinct()

date_df = date_df \
    .withColumn("year", year("order_date")) \
    .withColumn("month", month("order_date")) \
    .withColumn("day", dayofmonth("order_date")) \
    .withColumn("quarter", quarter("order_date"))

date_df.write.format("delta").mode("overwrite").save(GOLD_PATH)
