"""
data_loader.py
----------------
Data access layer for the E-Commerce Analytics Dashboard.

Tries to connect to the MongoDB collections produced by the Gold layer
of the pipeline (fact_invoices, dim_customers, dim_products). If no
MongoDB credentials are configured, it transparently falls back to the
bundled demo dataset (sample_data/demo_data.json) so the app is fully
runnable out-of-the-box for review purposes.

Environment variables (same names used in the pipeline notebook):
    MONGO_USERNAME
    MONGO_PASSWORD
    MONGO_CLUSTER
    MONGO_DATABASE
"""

import os
import json
from pathlib import Path
from functools import lru_cache

DEMO_DATA_PATH = Path(__file__).parent / "sample_data" / "demo_data.json"


def _load_demo_data() -> dict:
    with open(DEMO_DATA_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


@lru_cache(maxsize=1)
def get_mongo_client():
    """Return a connected pymongo client, or None if not configured/reachable."""
    username = os.environ.get("MONGO_USERNAME")
    password = os.environ.get("MONGO_PASSWORD")
    cluster = os.environ.get("MONGO_CLUSTER")
    database = os.environ.get("MONGO_DATABASE")

    if not all([username, password, cluster, database]):
        return None

    try:
        from pymongo import MongoClient
        uri = (
            f"mongodb+srv://{username}:{password}@{cluster}/"
            f"?retryWrites=true&w=majority"
        )
        client = MongoClient(uri, serverSelectionTimeoutMS=4000)
        client.admin.command("ping")  # fail fast if unreachable
        return client
    except Exception:
        return None


def is_live() -> bool:
    """True if the app is serving real data from MongoDB, False if demo mode."""
    return get_mongo_client() is not None


def get_kpis() -> dict:
    client = get_mongo_client()
    if client is None:
        demo = _load_demo_data()
        return {**demo["kpis"], "source": "demo"}

    db = client[os.environ["MONGO_DATABASE"]]
    total_orders = db.fact_invoices.count_documents({})
    active_customers = db.dim_customers.count_documents({})

    pipeline = [
        {"$group": {
            "_id": None,
            "total_revenue": {"$sum": "$total_revenue"},
            "avg_order_value": {"$avg": "$total_revenue"},
        }}
    ]
    agg = list(db.fact_invoices.aggregate(pipeline))
    total_revenue = agg[0]["total_revenue"] if agg else 0
    avg_order_value = agg[0]["avg_order_value"] if agg else 0

    return {
        "total_orders": total_orders,
        "active_customers": active_customers,
        "total_revenue_estimated": round(total_revenue, 2),
        "avg_order_value": round(avg_order_value, 2),
        "source": "live",
    }


def get_geographic_distribution() -> list:
    client = get_mongo_client()
    if client is None:
        return _load_demo_data()["geographic_distribution"]

    db = client[os.environ["MONGO_DATABASE"]]
    pipeline = [
        {"$group": {"_id": "$country", "transactions": {"$sum": 1}}},
        {"$sort": {"transactions": -1}},
        {"$limit": 10},
    ]
    results = list(db.fact_invoices.aggregate(pipeline))
    total = sum(r["transactions"] for r in results)
    return [
        {
            "country": r["_id"],
            "transactions": r["transactions"],
            "share_pct": round(r["transactions"] / total * 100, 2) if total else 0,
        }
        for r in results
    ]


def get_top_products(limit: int = 10) -> list:
    client = get_mongo_client()
    if client is None:
        return _load_demo_data()["top_products"][:limit]

    db = client[os.environ["MONGO_DATABASE"]]
    cursor = db.dim_products.find().sort("total_quantity_sold", -1).limit(limit)
    return [
        {
            "rank": i + 1,
            "stock_code": p.get("stock_code"),
            "description": p.get("description"),
            "quantity_sold": p.get("total_quantity_sold"),
        }
        for i, p in enumerate(cursor)
    ]


def search_customer(customer_id: str) -> dict | None:
    client = get_mongo_client()
    if client is None:
        matches = [
            c for c in _load_demo_data()["sample_customers"]
            if customer_id.strip().upper() in c["customer_id"].upper()
        ]
        return matches[0] if matches else None

    db = client[os.environ["MONGO_DATABASE"]]
    return db.dim_customers.find_one({"customer_id": customer_id}, {"_id": 0})


def search_invoice(invoice_id: str) -> dict | None:
    client = get_mongo_client()
    if client is None:
        matches = [
            i for i in _load_demo_data()["sample_invoices"]
            if invoice_id.strip().upper() in i["invoice_id"].upper()
        ]
        return matches[0] if matches else None

    db = client[os.environ["MONGO_DATABASE"]]
    return db.fact_invoices.find_one({"invoice_id": invoice_id}, {"_id": 0})


def get_monthly_revenue() -> list:
    client = get_mongo_client()
    if client is None:
        return _load_demo_data()["monthly_revenue"]

    db = client[os.environ["MONGO_DATABASE"]]
    pipeline = [
        {"$group": {
            "_id": {"year": "$year", "month": "$month"},
            "total_orders": {"$sum": 1},
            "total_revenue": {"$sum": "$total_revenue"},
        }},
        {"$sort": {"_id.year": 1, "_id.month": 1}},
    ]
    return list(db.fact_invoices.aggregate(pipeline))
