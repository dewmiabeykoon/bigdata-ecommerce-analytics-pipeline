"""
E-Commerce Analytics Dashboard
--------------------------------
Front-end for the Big Data E-Commerce Pipeline (Bronze -> Silver -> Gold -> MongoDB).

Run:
    streamlit run app.py

Works in two modes:
  - LIVE  : set MONGO_USERNAME / MONGO_PASSWORD / MONGO_CLUSTER / MONGO_DATABASE
            env vars to pull real figures from the Gold-layer MongoDB collections.
  - DEMO  : with no MongoDB configured, the app runs immediately using the
            bundled sample dataset so reviewers can try it with zero setup.
"""

import streamlit as st
import pandas as pd
import plotly.express as px

import data_loader as dl

st.set_page_config(
    page_title="E-Commerce Analytics Dashboard",
    page_icon="🛒",
    layout="wide",
)

# ---------------------------------------------------------------------------
# Header + mode banner
# ---------------------------------------------------------------------------
st.title("🛒 E-Commerce Analytics Dashboard")
st.caption("Bronze → Silver → Gold pipeline output, served from MongoDB")

if dl.is_live():
    st.success("🟢 Connected to live MongoDB data", icon="✅")
else:
    st.info(
        "🟡 **Demo Mode** — no MongoDB connection configured, showing sample "
        "data grounded in the project's published pipeline metrics. "
        "Set `MONGO_USERNAME`, `MONGO_PASSWORD`, `MONGO_CLUSTER`, `MONGO_DATABASE` "
        "to connect to a live database.",
        icon="ℹ️",
    )

tab_overview, tab_geo, tab_explorer = st.tabs(
    ["📊 Executive Overview", "🌍 Geographic Analytics", "🔎 Data Explorer"]
)

# ---------------------------------------------------------------------------
# TAB 1 — Executive Overview (KPIs)
# ---------------------------------------------------------------------------
with tab_overview:
    kpis = dl.get_kpis()

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total Revenue", f"£{kpis['total_revenue_estimated']:,.2f}")
    col2.metric("Total Orders", f"{kpis['total_orders']:,}")
    col3.metric("Active Customers", f"{kpis['active_customers']:,}")
    col4.metric("Avg. Order Value", f"£{kpis['avg_order_value']:,.2f}")

    st.divider()
    st.subheader("Monthly Revenue Trend")

    monthly = pd.DataFrame(dl.get_monthly_revenue())
    if not monthly.empty:
        fig = px.bar(
            monthly,
            x="month_name" if "month_name" in monthly.columns else "year_month",
            y="total_revenue",
            text_auto=".2s",
            labels={"total_revenue": "Revenue (£)", "month_name": "Month"},
            color_discrete_sequence=["#2563eb"],
        )
        fig.update_layout(height=380, margin=dict(t=10, b=10))
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.warning("No monthly revenue data available.")

    st.subheader("Top 10 Best-Selling Products")
    products = pd.DataFrame(dl.get_top_products(10))
    if not products.empty:
        fig2 = px.bar(
            products.sort_values("quantity_sold"),
            x="quantity_sold",
            y="description",
            orientation="h",
            labels={"quantity_sold": "Units Sold", "description": "Product"},
            color_discrete_sequence=["#f59e0b"],
        )
        fig2.update_layout(height=420, margin=dict(t=10, b=10))
        st.plotly_chart(fig2, use_container_width=True)

# ---------------------------------------------------------------------------
# TAB 2 — Geographic Analytics
# ---------------------------------------------------------------------------
with tab_geo:
    st.subheader("Transactions by Country")

    geo = pd.DataFrame(dl.get_geographic_distribution())
    if not geo.empty:
        fig3 = px.bar(
            geo.sort_values("transactions"),
            x="transactions",
            y="country",
            orientation="h",
            text="transactions",
            labels={"transactions": "Transactions", "country": "Country"},
            color="transactions",
            color_continuous_scale="Blues",
        )
        fig3.update_traces(texttemplate="%{text:,}", textposition="outside")
        fig3.update_layout(height=460, margin=dict(t=10, b=10), coloraxis_showscale=False)
        st.plotly_chart(fig3, use_container_width=True)

        with st.expander("View as table"):
            st.dataframe(
                geo.rename(columns={
                    "country": "Country",
                    "transactions": "Transactions",
                    "share_pct": "Share (%)",
                }),
                use_container_width=True,
                hide_index=True,
            )

        uk_share = geo.iloc[0]["share_pct"] if not geo.empty else 0
        st.caption(
            f"📌 The top market ({geo.iloc[0]['country']}) accounts for "
            f"**{uk_share:.1f}%** of transactions — highlighting geographic "
            f"revenue concentration and international growth opportunity."
        )
    else:
        st.warning("No geographic data available.")

# ---------------------------------------------------------------------------
# TAB 3 — Data Explorer / Search
# ---------------------------------------------------------------------------
with tab_explorer:
    st.subheader("Search Customers & Invoices")
    st.caption(
        "Try the demo IDs shown below the search boxes, or search your own "
        "IDs if connected to a live database."
    )

    search_type = st.radio(
        "Search by", ["Customer ID", "Invoice ID"], horizontal=True
    )

    if search_type == "Customer ID":
        query = st.text_input(
            "Customer ID",
            placeholder="e.g. 17850" if dl.is_live() else "e.g. DEMO-17850",
        )
        if not dl.is_live():
            st.caption("Demo IDs to try: DEMO-17850, DEMO-14606, DEMO-15311")
        if query:
            result = dl.search_customer(query)
            if result:
                c1, c2, c3, c4 = st.columns(4)
                c1.metric("Total Orders", result.get("total_orders", "—"))
                c2.metric("Total Revenue", f"£{result.get('total_revenue', 0):,.2f}")
                c3.metric("Avg. Order Value", f"£{result.get('average_order_value', 0):,.2f}")
                c4.metric("Recency (days)", result.get("recency_days", result.get("recency", "—")))
                st.json(result)
            else:
                st.warning("No customer found with that ID.")

    else:
        query = st.text_input(
            "Invoice ID",
            placeholder="e.g. 536365" if dl.is_live() else "e.g. DEMO-536365",
        )
        if not dl.is_live():
            st.caption("Demo IDs to try: DEMO-536365, DEMO-536406, DEMO-C536379")
        if query:
            result = dl.search_invoice(query)
            if result:
                c1, c2, c3 = st.columns(3)
                c1.metric("Total Revenue", f"£{result.get('total_revenue', 0):,.2f}")
                c2.metric("Items", result.get("total_items", "—"))
                c3.metric("Has Returns", "Yes" if result.get("has_returns") else "No")
                st.json(result)
            else:
                st.warning("No invoice found with that ID.")

st.divider()
st.caption(
    "Data pipeline: PySpark (Bronze/Silver/Gold medallion architecture) → "
    "MongoDB Atlas. See the [full project report](docs/Final_Report.pdf) for methodology."
)
