"""
silver_layer.py
------------------
SECTION 3 of the pipeline: data cleaning.

Applies a seven-step quality process to the Bronze layer and writes the
result as partitioned Parquet -- the Silver layer. Also produces a data
quality report (CSV) documenting what was removed at each step.
"""

import pandas as pd
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, when

from config import SILVER_PATH, REPORTS_PATH


def remove_null_customer_ids(df: DataFrame) -> tuple[DataFrame, int]:
    before = df.count()
    df = df.filter(col("Customer ID").isNotNull())
    removed = before - df.count()
    print(f"[Silver] Step 1 - Removed null Customer IDs: {removed:,}")
    return df, removed


def remove_cancelled_invoices(df: DataFrame) -> tuple[DataFrame, int]:
    before = df.count()
    df = df.filter(~col("Invoice").startswith("C"))
    removed = before - df.count()
    print(f"[Silver] Step 2 - Removed cancelled invoices: {removed:,}")
    return df, removed


def remove_invalid_prices(df: DataFrame) -> tuple[DataFrame, int]:
    before = df.count()
    df = df.filter((col("Price") > 0) & (col("Price").isNotNull()))
    removed = before - df.count()
    print(f"[Silver] Step 3 - Removed invalid prices: {removed:,}")
    return df, removed


def flag_returns(df: DataFrame) -> DataFrame:
    df = df.withColumn("IsReturn", when(col("Quantity") < 0, True).otherwise(False))
    return_count = df.filter(col("IsReturn") == True).count()  # noqa: E712
    print(f"[Silver] Step 4 - Flagged returns: {return_count:,}")
    return df


def remove_duplicates(df: DataFrame) -> tuple[DataFrame, int]:
    before = df.count()
    df = df.dropDuplicates()
    removed = before - df.count()
    print(f"[Silver] Step 5 - Removed duplicates: {removed:,}")
    return df, removed


def remove_quantity_outliers(df: DataFrame, threshold: int = 10000) -> tuple[DataFrame, int]:
    before = df.count()
    df = df.filter((col("Quantity") <= threshold) & (col("Quantity") >= -threshold))
    removed = before - df.count()
    print(f"[Silver] Step 6 - Removed quantity outliers: {removed:,}")
    return df, removed


def remove_null_descriptions(df: DataFrame) -> tuple[DataFrame, int]:
    before = df.count()
    df = df.filter(col("Description").isNotNull())
    removed = before - df.count()
    print(f"[Silver] Step 7 - Removed null descriptions: {removed:,}")
    return df, removed


def save_quality_report(bronze_total: int, removals: dict, silver_total: int,
                         output_path: str = REPORTS_PATH) -> None:
    quality_pct = round(silver_total / bronze_total * 100, 2)
    data = {
        "Metric": [
            "Bronze Layer Total Records",
            "Records with Null Customer ID",
            "Cancelled Invoices",
            "Invalid Prices",
            "Duplicate Records",
            "Quantity Outliers",
            "Null Descriptions",
            "Silver Layer Total Records",
            "Total Records Removed",
            "Data Quality Rate (%)",
        ],
        "Count": [
            bronze_total,
            removals["nulls"],
            removals["cancelled"],
            removals["price"],
            removals["duplicates"],
            removals["outliers"],
            removals["descriptions"],
            silver_total,
            bronze_total - silver_total,
            quality_pct,
        ],
    }
    pd.DataFrame(data).to_csv(f"{output_path}/data_quality_report.csv", index=False)
    print(f"[Silver] Data quality report saved to: {output_path}/data_quality_report.csv")


def save(df_silver: DataFrame, output_path: str = SILVER_PATH) -> None:
    df_silver.write.mode("overwrite").partitionBy("Year", "Month").parquet(output_path)
    print(f"[Silver] Saved to: {output_path}")


def run(spark: SparkSession, bronze_path: str, output_path: str = SILVER_PATH) -> DataFrame:
    """Run the full Silver layer cleaning stage and return the resulting DataFrame."""
    df = spark.read.parquet(bronze_path)
    bronze_total = df.count()
    print(f"[Silver] Loaded Bronze layer: {bronze_total:,} records")

    df, removed_nulls = remove_null_customer_ids(df)
    df, removed_cancelled = remove_cancelled_invoices(df)
    df, removed_price = remove_invalid_prices(df)
    df = flag_returns(df)
    df, removed_duplicates = remove_duplicates(df)
    df, removed_outliers = remove_quantity_outliers(df)
    df, removed_descriptions = remove_null_descriptions(df)

    silver_total = df.count()
    print(f"[Silver] Final record count: {silver_total:,}")

    save_quality_report(
        bronze_total,
        {
            "nulls": removed_nulls,
            "cancelled": removed_cancelled,
            "price": removed_price,
            "duplicates": removed_duplicates,
            "outliers": removed_outliers,
            "descriptions": removed_descriptions,
        },
        silver_total,
    )

    save(df, output_path)
    return df
