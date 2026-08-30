"""
gold_layer.py
---------------
SECTION 4 of the pipeline: feature engineering.

Builds the analytics-ready feature set on top of the Silver layer:
Revenue, time-based features, basket size, customer total spend
(window function), and product popularity rank. Writes the result as
partitioned Parquet -- the Gold layer.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.window import Window
from pyspark.sql.functions import (
    col, hour, dayofweek, date_format, count, sum as spark_sum,
    desc, row_number, abs as spark_abs, when,
)

from config import GOLD_PATH


def add_revenue(df: DataFrame) -> DataFrame:
    return df.withColumn("Revenue", col("Quantity") * col("Price"))


def add_time_features(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("Hour", hour(col("InvoiceDate")))
        .withColumn("DayOfWeek", dayofweek(col("InvoiceDate")))
        .withColumn("DayName", date_format(col("InvoiceDate"), "EEEE"))
        .withColumn("MonthName", date_format(col("InvoiceDate"), "MMMM"))
    )


def add_basket_size(df: DataFrame) -> DataFrame:
    basket_size = df.groupBy("Invoice").agg(count("*").alias("BasketSize"))
    return df.join(basket_size, "Invoice", "left")


def add_customer_total_spend(df: DataFrame) -> DataFrame:
    """Window function: total revenue per customer, attached to every row."""
    customer_window = Window.partitionBy("Customer ID")
    return df.withColumn("CustomerTotalSpend", spark_sum("Revenue").over(customer_window))


def add_product_popularity_rank(df: DataFrame) -> DataFrame:
    product_sales = (
        df.filter(col("Quantity") > 0)
        .groupBy("StockCode", "Description")
        .agg(spark_sum("Quantity").alias("TotalQuantitySold"))
    )
    product_window = Window.orderBy(desc("TotalQuantitySold"))
    product_sales = product_sales.withColumn("PopularityRank", row_number().over(product_window))
    return df.join(product_sales.select("StockCode", "PopularityRank"), "StockCode", "left")


def add_helper_features(df: DataFrame) -> DataFrame:
    df = df.withColumn("AbsoluteQuantity", spark_abs(col("Quantity")))
    df = df.withColumn(
        "TransactionType", when(col("IsReturn") == True, "Return").otherwise("Sale")  # noqa: E712
    )
    return df


def save(df_gold: DataFrame, output_path: str = GOLD_PATH) -> None:
    df_gold.write.mode("overwrite").partitionBy("Year", "Month").parquet(output_path)
    print(f"[Gold] Saved to: {output_path}")


def run(spark: SparkSession, silver_path: str, output_path: str = GOLD_PATH) -> DataFrame:
    """Run the full Gold layer feature-engineering stage and return the resulting DataFrame."""
    df = spark.read.parquet(silver_path)
    print(f"[Gold] Loaded Silver layer: {df.count():,} records")

    df = add_revenue(df)
    df = add_time_features(df)
    df = add_basket_size(df)
    df = add_customer_total_spend(df)
    df = add_product_popularity_rank(df)
    df = add_helper_features(df)

    print(f"[Gold] Feature engineering complete. Total records: {df.count():,}")
    save(df, output_path)
    return df
