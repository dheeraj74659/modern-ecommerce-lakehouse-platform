# config/pipeline_config.py

PIPELINE_CONFIG = {
    "bronze": {
        "source_path": "s3://ecommerce-raw-data/orders/",
        "target_path": "s3://ecommerce-lakehouse/bronze/orders/",
        "checkpoint_table": "metadata.pipeline_watermarks",
        "primary_key": "order_id",
        "watermark_column": "updated_at",
        "partition_column": "order_date"
    },
    "silver": {
        "source_path": "s3://ecommerce-lakehouse/bronze/orders/",
        "target_path": "s3://ecommerce-lakehouse/silver/orders/",
        "primary_key": "order_id",
        "watermark_column": "updated_at",
        "partition_column": "order_date"
    }
}
