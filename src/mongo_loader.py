"""
mongo_loader.py
-----------------
SECTION 5 of the pipeline: MongoDB data modeling and loading.

Builds three MongoDB collections from the Gold layer:
  - fact_invoices  : invoice-level fact table with embedded line items
  - dim_customers  : customer dimension with RFM metrics and segmentation
  - dim_products   : product dimension with sales performance metrics

Then writes them to MongoDB in batches via PyMongo.
"""

import json
import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.window import Window
from pyspark.sql.functions import (
    col, first, count, sum as spark_sum, avg, min as spark_min, max as spark_max,
    desc, lit, datediff, ntile, when, collect_list, struct, array_distinct, size,
    row_number, countDistinct, abs as spark_abs,
)
from pymongo import MongoClient

from config import get_mongo_uri, get_mongo_database_name


# ---------------------------------------------------------------------------
# Collection builders
# ---------------------------------------------------------------------------

def build_fact_invoices(df_gold: DataFrame) -> DataFrame:
    """Invoice-level fact table with embedded line-item arrays."""
    df_line_items = df_gold.select(
        "Invoice",
        struct(
            col("StockCode").alias("stock_code"),
            col("Description").alias("description"),
            col("Quantity").alias("quantity"),
            col("Price").alias("price"),
            col("Revenue").alias("revenue"),
            col("IsReturn").alias("is_return"),
        ).alias("line_item"),
    )

    df_invoice_items = df_line_items.groupBy("Invoice").agg(
        collect_list("line_item").alias("line_items")
    )

    df_fact_invoices = df_gold.groupBy("Invoice").agg(
        first("Customer ID").alias("customer_id"),
        first("Country").alias("country"),
        first("InvoiceDate").alias("invoice_date"),
        first("Year").alias("year"),
        first("Month").alias("month"),
        first("Hour").alias("hour"),
        first("DayName").alias("day_name"),
        count("*").alias("total_items"),
        spark_sum("Revenue").alias("total_revenue"),
        F.max("IsReturn").alias("has_returns"),
    )

    df_fact_invoices = df_fact_invoices.join(df_invoice_items, "Invoice", "left")
    df_fact_invoices = df_fact_invoices.withColumnRenamed("Invoice", "invoice_id")

    print(f"[Mongo] fact_invoices built: {df_fact_invoices.count():,} documents")
    return df_fact_invoices


def build_dim_customers(df_gold: DataFrame) -> DataFrame:
    """Customer dimension with RFM metrics and segmentation."""
    df_customer_sales = df_gold.filter(col("IsReturn") == False)  # noqa: E712

    df_dim_customers = df_customer_sales.groupBy("Customer ID").agg(
        first("Country").alias("country"),
        countDistinct("Invoice").alias("total_orders"),
        spark_sum("Quantity").alias("total_items_purchased"),
        spark_sum("Revenue").alias("total_revenue"),
        avg("Revenue").alias("average_basket_value"),
        avg("Quantity").alias("average_basket_size"),
        spark_min("InvoiceDate").alias("first_purchase_date"),
        spark_max("InvoiceDate").alias("last_purchase_date"),
    )

    max_date_in_data = df_customer_sales.agg(spark_max("InvoiceDate")).first()[0]
    df_dim_customers = df_dim_customers.withColumn(
        "recency_days", datediff(lit(max_date_in_data), col("last_purchase_date"))
    )

    df_dim_customers = df_dim_customers.withColumnRenamed("total_orders", "frequency")
    df_dim_customers = df_dim_customers.withColumn("monetary", col("total_revenue"))

    recency_window = Window.orderBy(col("recency_days"))
    frequency_window = Window.orderBy(desc("frequency"))
    monetary_window = Window.orderBy(desc("monetary"))

    df_dim_customers = (
        df_dim_customers.withColumn("r_score", ntile(5).over(recency_window))
        .withColumn("f_score", ntile(5).over(frequency_window))
        .withColumn("m_score", ntile(5).over(monetary_window))
    )

    df_dim_customers = df_dim_customers.withColumn(
        "rfm_score", ((col("r_score") + col("f_score") + col("m_score")) / 3).cast("int")
    )

    df_dim_customers = df_dim_customers.withColumn(
        "customer_segment",
        when(col("rfm_score") >= 4, "High Value")
        .when(col("rfm_score") >= 3, "Medium Value")
        .otherwise("Low Value"),
    )

    df_dim_customers = df_dim_customers.withColumnRenamed("Customer ID", "customer_id")

    df_dim_customers = df_dim_customers.select(
        "customer_id", "country", "frequency", "total_items_purchased",
        "total_revenue", "average_basket_value", "average_basket_size",
        "first_purchase_date", "last_purchase_date", "recency_days",
        "monetary", "rfm_score", "customer_segment",
    )

    print(f"[Mongo] dim_customers built: {df_dim_customers.count():,} documents")
    return df_dim_customers


def build_dim_products(df_gold: DataFrame) -> DataFrame:
    """Product dimension with sales performance and return metrics."""
    df_product_sales = df_gold.filter(col("IsReturn") == False)  # noqa: E712
    df_product_returns = df_gold.filter(col("IsReturn") == True)  # noqa: E712

    df_product_metrics = df_product_sales.groupBy("StockCode", "Description").agg(
        spark_sum("Quantity").alias("total_quantity_sold"),
        spark_sum("Revenue").alias("total_revenue"),
        countDistinct("Invoice").alias("total_orders"),
        avg("Price").alias("average_price"),
        collect_list("Country").alias("all_countries"),
    )

    df_product_return_metrics = df_product_returns.groupBy("StockCode").agg(
        spark_sum(spark_abs(col("Quantity"))).alias("total_returns")
    )

    df_dim_products = df_product_metrics.join(df_product_return_metrics, "StockCode", "left")
    df_dim_products = df_dim_products.fillna({"total_returns": 0})

    df_dim_products = df_dim_products.withColumn(
        "return_rate",
        col("total_returns") / (col("total_quantity_sold") + col("total_returns")) * 100,
    )

    df_dim_products = df_dim_products.withColumn(
        "countries_sold", array_distinct(col("all_countries"))
    ).withColumn("country_count", size(col("countries_sold")))

    product_rank_window = Window.orderBy(desc("total_quantity_sold"))
    df_dim_products = df_dim_products.withColumn(
        "popularity_rank", row_number().over(product_rank_window)
    )

    df_dim_products = df_dim_products.withColumnRenamed(
        "StockCode", "stock_code"
    ).withColumnRenamed("Description", "description")

    df_dim_products = df_dim_products.select(
        "stock_code", "description", "total_quantity_sold", "total_revenue",
        "total_orders", "average_price", "total_returns", "return_rate",
        "countries_sold", "country_count", "popularity_rank",
    )

    print(f"[Mongo] dim_products built: {df_dim_products.count():,} documents")
    return df_dim_products


# ---------------------------------------------------------------------------
# Writing to MongoDB
# ---------------------------------------------------------------------------

def write_collection(db, collection_name: str, df: DataFrame, batch_size: int = 5000) -> None:
    """Clear the target collection and write a DataFrame to it in batches."""
    db[collection_name].delete_many({})

    total = df.count()
    pdf = df.toPandas()
    records = json.loads(pdf.to_json(orient="records", date_format="iso"))

    for i in range(0, len(records), batch_size):
        batch = records[i : i + batch_size]
        if batch:
            db[collection_name].insert_many(batch)
        print(f"[Mongo] {collection_name}: wrote batch {i // batch_size + 1} "
              f"({min(i + batch_size, total):,}/{total:,})")

    inserted = db[collection_name].count_documents({})
    print(f"[Mongo] {collection_name} complete: {inserted:,} documents")


def create_indexes(db) -> None:
    """Create the indexes used by the analytics queries and dashboard."""
    db.fact_invoices.create_index("customer_id")
    db.fact_invoices.create_index("invoice_date")
    db.dim_customers.create_index([("total_revenue", -1)])
    db.dim_products.create_index([("total_revenue", -1)])
    print("[Mongo] Indexes created: fact_invoices.customer_id, "
          "fact_invoices.invoice_date, dim_customers.total_revenue (desc), "
          "dim_products.total_revenue (desc)")


def run(df_gold: DataFrame) -> None:
    """Build all three collections and write them to MongoDB."""
    df_fact_invoices = build_fact_invoices(df_gold)
    df_dim_customers = build_dim_customers(df_gold)
    df_dim_products = build_dim_products(df_gold)

    client = MongoClient(get_mongo_uri())
    db = client[get_mongo_database_name()]
    print("[Mongo] Connected to MongoDB")

    write_collection(db, "fact_invoices", df_fact_invoices)
    write_collection(db, "dim_customers", df_dim_customers)
    write_collection(db, "dim_products", df_dim_products)

    create_indexes(db)
    client.close()
