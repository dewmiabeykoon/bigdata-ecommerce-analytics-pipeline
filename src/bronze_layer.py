"""
bronze_layer.py
-----------------
SECTION 2 of the pipeline: raw data ingestion.

Reads the raw e-commerce CSV, corrects data types (InvoiceDate -> timestamp),
adds Year/Month partitioning columns, and writes the result as partitioned
Parquet -- the Bronze layer.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, to_timestamp, year, month, count, desc

from config import BRONZE_PATH


def load_raw_data(spark: SparkSession, csv_path: str) -> DataFrame:
    """Read the raw CSV into a Spark DataFrame."""
    df_raw = spark.read.csv(csv_path, header=True, inferSchema=True)
    print(f"[Bronze] Loaded raw dataset: {df_raw.count():,} records")
    return df_raw


def correct_types(df_raw: DataFrame) -> DataFrame:
    """Convert InvoiceDate from string to timestamp."""
    df_bronze = df_raw.withColumn(
        "InvoiceDate", to_timestamp(col("InvoiceDate"), "M/d/yy H:mm")
    )
    null_dates = df_bronze.filter(col("InvoiceDate").isNull()).count()
    print(f"[Bronze] Type correction complete. Null InvoiceDate records: {null_dates:,}")
    return df_bronze


def add_partition_columns(df_bronze: DataFrame) -> DataFrame:
    """Add Year/Month columns used for Parquet partitioning."""
    df_bronze = df_bronze.withColumn("Year", year(col("InvoiceDate"))).withColumn(
        "Month", month(col("InvoiceDate"))
    )
    return df_bronze


def summarize(df_bronze: DataFrame) -> None:
    """Print Bronze layer summary statistics."""
    print("\n=== BRONZE LAYER SUMMARY ===")
    print(f"Total Records: {df_bronze.count():,}")
    print(f"Total Columns: {len(df_bronze.columns)}")
    print("\nTop 10 Countries by Transaction Count:")
    df_bronze.groupBy("Country").agg(count("*").alias("Transaction_Count")).orderBy(
        desc("Transaction_Count")
    ).show(10)


def save(df_bronze: DataFrame, output_path: str = BRONZE_PATH) -> None:
    df_bronze.write.mode("overwrite").partitionBy("Year", "Month").parquet(output_path)
    print(f"[Bronze] Saved to: {output_path}")


def run(spark: SparkSession, csv_path: str, output_path: str = BRONZE_PATH) -> DataFrame:
    """Run the full Bronze layer stage and return the resulting DataFrame."""
    df_raw = load_raw_data(spark, csv_path)
    df_bronze = correct_types(df_raw)
    df_bronze = add_partition_columns(df_bronze)
    summarize(df_bronze)
    save(df_bronze, output_path)
    return df_bronze
