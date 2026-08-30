"""
config.py
----------
Shared configuration: Spark session creation and path/environment setup
for the Bronze -> Silver -> Gold -> MongoDB pipeline.

Reads MongoDB credentials from environment variables (works both locally
and in Colab, as long as the variables are exported before running):

    MONGO_USERNAME
    MONGO_PASSWORD
    MONGO_CLUSTER
    MONGO_DATABASE
"""

import os
from pyspark.sql import SparkSession


def get_env_or_raise(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise EnvironmentError(
            f"Missing required environment variable: {name}. "
            f"Set it before running the pipeline."
        )
    return value


def get_mongo_uri() -> str:
    username = get_env_or_raise("MONGO_USERNAME")
    password = get_env_or_raise("MONGO_PASSWORD")
    cluster = get_env_or_raise("MONGO_CLUSTER")
    database = get_env_or_raise("MONGO_DATABASE")
    return f"mongodb+srv://{username}:{password}@{cluster}/{database}?retryWrites=true&w=majority"


def get_mongo_database_name() -> str:
    return get_env_or_raise("MONGO_DATABASE")


def create_spark_session(app_name: str = "ECommerce_Pipeline") -> SparkSession:
    """Create (or fetch) the Spark session, configured with the MongoDB connector."""
    mongo_uri = get_mongo_uri()

    spark = (
        SparkSession.builder.appName(app_name)
        .config("spark.mongodb.read.connection.uri", mongo_uri)
        .config("spark.mongodb.write.connection.uri", mongo_uri)
        .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.1.1")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .getOrCreate()
    )
    return spark


# Base path for pipeline artifacts (Parquet layers, reports).
# Override with the PIPELINE_BASE_PATH env var; defaults to a local ./data folder.
BASE_PATH = os.environ.get("PIPELINE_BASE_PATH", "./data")

BRONZE_PATH = f"{BASE_PATH}/bronze_layer/ecommerce_data"
SILVER_PATH = f"{BASE_PATH}/silver_layer/ecommerce_cleaned"
GOLD_PATH = f"{BASE_PATH}/gold_layer/ecommerce_features"
REPORTS_PATH = f"{BASE_PATH}/reports"
