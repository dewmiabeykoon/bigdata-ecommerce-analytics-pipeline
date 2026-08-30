#!/usr/bin/env python3
"""
run_pipeline.py
------------------
Orchestrates the Big Data E-Commerce Pipeline end-to-end:

    Raw CSV -> Bronze -> Silver -> Gold -> MongoDB (fact_invoices,
    dim_customers, dim_products)

Usage:
    python run_pipeline.py --csv data/online_retail.csv
    python run_pipeline.py --csv data/online_retail.csv --stage bronze
    python run_pipeline.py --csv data/online_retail.csv --stage silver
    python run_pipeline.py --csv data/online_retail.csv --stage gold
    python run_pipeline.py --stage mongo

Required environment variables (MongoDB connection):
    MONGO_USERNAME, MONGO_PASSWORD, MONGO_CLUSTER, MONGO_DATABASE

Optional:
    PIPELINE_BASE_PATH   Where Bronze/Silver/Gold Parquet + reports are
                          written. Defaults to ./data
"""

import argparse
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "src"))

from config import (  # noqa: E402
    create_spark_session, BRONZE_PATH, SILVER_PATH, GOLD_PATH, REPORTS_PATH, BASE_PATH,
)
import bronze_layer  # noqa: E402
import silver_layer  # noqa: E402
import gold_layer  # noqa: E402
import mongo_loader  # noqa: E402


def ensure_dirs() -> None:
    for path in [f"{BASE_PATH}/bronze_layer", f"{BASE_PATH}/silver_layer",
                 f"{BASE_PATH}/gold_layer", REPORTS_PATH]:
        os.makedirs(path, exist_ok=True)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the Big Data E-Commerce Pipeline")
    parser.add_argument("--csv", help="Path to the raw online_retail.csv (required for 'bronze'/'all')")
    parser.add_argument(
        "--stage",
        choices=["bronze", "silver", "gold", "mongo", "all"],
        default="all",
        help="Which stage to run (default: all)",
    )
    args = parser.parse_args()

    ensure_dirs()
    spark = create_spark_session()

    if args.stage in ("bronze", "all"):
        if not args.csv:
            parser.error("--csv is required to run the 'bronze' stage")
        bronze_layer.run(spark, args.csv, BRONZE_PATH)

    if args.stage in ("silver", "all"):
        silver_layer.run(spark, BRONZE_PATH, SILVER_PATH)

    if args.stage in ("gold", "all"):
        gold_layer.run(spark, SILVER_PATH, GOLD_PATH)

    if args.stage in ("mongo", "all"):
        df_gold = spark.read.parquet(GOLD_PATH)
        mongo_loader.run(df_gold)

    print("\n[Pipeline] Done.")
    spark.stop()


if __name__ == "__main__":
    main()
