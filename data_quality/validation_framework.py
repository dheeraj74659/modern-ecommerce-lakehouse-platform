# data_quality/validation_framework.py

import logging
from pyspark.sql.functions import col, count, sum as spark_sum
from utils.spark_session import create_spark_session

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("DataQualityFramework")

spark = create_spark_session("Data-Quality-Checks")


class DataQualityValidator:

    def __init__(self):
        self.failed_checks = []

    # -------------------------------------------------------
    # 1. Row Count Validation
    # -------------------------------------------------------
    def validate_row_count(self, source_df, target_df, tolerance=0.02):
        """
        Ensures row counts between layers are within acceptable tolerance.
        Default tolerance: 2% difference.
        """
        source_count = source_df.count()
        target_count = target_df.count()

        if source_count == 0:
            self.failed_checks.append("Source row count is zero.")
            return

        diff_ratio = abs(source_count - target_count) / source_count

        if diff_ratio > tolerance:
            self.failed_checks.append(
                f"Row count mismatch beyond tolerance: {diff_ratio}"
            )

    # -------------------------------------------------------
    # 2. Null Check
    # -------------------------------------------------------
    def validate_not_null(self, df, column_list):
        """
        Ensures critical columns do not contain NULL values.
        """
        for column in column_list:
            null_count = df.filter(col(column).isNull()).count()
            if null_count > 0:
                self.failed_checks.append(
                    f"Column {column} contains {null_count} NULL values."
                )

    # -------------------------------------------------------
    # 3. Duplicate Check
    # -------------------------------------------------------
    def validate_no_duplicates(self, df, primary_key):
        """
        Ensures no duplicate primary keys exist.
        """
        dup_count = (
            df.groupBy(primary_key)
            .count()
            .filter(col("count") > 1)
            .count()
        )

        if dup_count > 0:
            self.failed_checks.append(
                f"Found {dup_count} duplicate primary keys."
            )

    # -------------------------------------------------------
    # 4. Referential Integrity Check
    # -------------------------------------------------------
    def validate_foreign_key(self, fact_df, dim_df, fact_key, dim_key):
        """
        Ensures foreign key references exist in dimension.
        """
        unmatched = fact_df.join(
            dim_df,
            fact_df[fact_key] == dim_df[dim_key],
            "left_anti"
        ).count()

        if unmatched > 0:
            self.failed_checks.append(
                f"{unmatched} foreign key violations detected."
            )

    # -------------------------------------------------------
    # 5. Schema Validation
    # -------------------------------------------------------
    def validate_schema(self, df, expected_schema):
        """
        Ensures schema has not drifted unexpectedly.
        """
        current_fields = set(df.columns)
        expected_fields = set(expected_schema)

        if current_fields != expected_fields:
            self.failed_checks.append(
                f"Schema drift detected. Expected: {expected_fields}, Found: {current_fields}"
            )

    # -------------------------------------------------------
    # Final Validation Gate
    # -------------------------------------------------------
    def validate(self):
        if self.failed_checks:
            for failure in self.failed_checks:
                logger.error(failure)
            raise Exception("Data Quality Validation Failed.")
        else:
            logger.info("All data quality checks passed successfully.")
