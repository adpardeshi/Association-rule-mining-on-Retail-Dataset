import os

import pandas as pd
import plotly.express as px
import streamlit as st
import psycopg2
from psycopg2 import sql


# ============================================================
# PAGE CONFIGURATION
# ============================================================

st.set_page_config(
    page_title="Instacart Market Basket Analysis",
    page_icon="🛒",
    layout="wide"
)


# ============================================================
# DATABASE CONFIGURATION
# ============================================================

DB_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "postgres"),
    "port": int(os.getenv("POSTGRES_PORT", "5432")),
    "database": os.getenv("POSTGRES_DB", "instacart"),
    "user": os.getenv("POSTGRES_USER", "postgres"),
    "password": os.getenv("POSTGRES_PASSWORD", "postgres"),
}


# ============================================================
# DATABASE CONNECTION
# ============================================================

@st.cache_resource
def get_connection():
    return psycopg2.connect(
        host=DB_CONFIG["host"],
        port=DB_CONFIG["port"],
        database=DB_CONFIG["database"],
        user=DB_CONFIG["user"],
        password=DB_CONFIG["password"]
    )


# ============================================================
# LOAD ASSOCIATION RULES
# ============================================================

@st.cache_data(ttl=60)
def load_rules():

    conn = get_connection()

    query = """
        SELECT
            antecedent,
            antecedent_product_names,
            consequent,
            consequent_product_names,
            confidence,
            lift,
            created_at
        FROM association_rules
        ORDER BY lift DESC
    """

    df = pd.read_sql_query(query, conn)

    return df


# ============================================================
# APPLICATION TITLE
# ============================================================

st.title("🛒 Instacart Market Basket Analysis Dashboard")

st.markdown(
    """
    This dashboard presents product association rules generated using
    **Apache Spark FP-Growth** from the Instacart Market Basket dataset.

    Use this dashboard to explore:
    - Association rule KPIs
    - Product recommendations
    - Confidence and lift
    - Top product associations
    """
)


# ============================================================
# LOAD DATA
# ============================================================

try:

    rules = load_rules()

except Exception as e:

    st.error("Unable to connect to PostgreSQL.")

    st.code(str(e))

    st.info(
        "Make sure PostgreSQL is running and the association_rules "
        "table has been populated."
    )

    st.stop()


# ============================================================
# CHECK DATA
# ============================================================

if rules.empty:

    st.warning(
        "No association rules found in PostgreSQL."
    )

    st.stop()


# ============================================================
# DATA CLEANING
# ============================================================

rules["confidence"] = pd.to_numeric(
    rules["confidence"],
    errors="coerce"
)

rules["lift"] = pd.to_numeric(
    rules["lift"],
    errors="coerce"
)

rules = rules.dropna(
    subset=["confidence", "lift"]
)


# ============================================================
# SIDEBAR
# ============================================================

st.sidebar.header("Dashboard Controls")

min_confidence = st.sidebar.slider(
    "Minimum Confidence",
    min_value=0.0,
    max_value=1.0,
    value=0.0,
    step=0.01
)

min_lift = st.sidebar.slider(
    "Minimum Lift",
    min_value=0.0,
    max_value=float(max(5.0, rules["lift"].max())),
    value=0.0,
    step=0.1
)


# ============================================================
# FILTER DATA
# ============================================================

filtered_rules = rules[
    (rules["confidence"] >= min_confidence)
    &
    (rules["lift"] >= min_lift)
]


# ============================================================
# KPI SECTION
# ============================================================

st.subheader("📊 Key Performance Indicators")

col1, col2, col3, col4 = st.columns(4)


with col1:

    st.metric(
        "Total Association Rules",
        f"{len(rules):,}"
    )


with col2:

    st.metric(
        "Filtered Rules",
        f"{len(filtered_rules):,}"
    )


with col3:

    st.metric(
        "Average Confidence",
        f"{rules['confidence'].mean():.2%}"
    )


with col4:

    st.metric(
        "Average Lift",
        f"{rules['lift'].mean():.2f}"
    )


st.divider()


# ============================================================
# PRODUCT RECOMMENDATION SEARCH
# ============================================================

st.subheader("🔎 Product Recommendation Search")

search_product = st.text_input(
    "Enter a product name",
    placeholder="Example: Banana"
)


if search_product:

    search_results = rules[
        rules["antecedent_product_names"]
        .str.contains(
            search_product,
            case=False,
            na=False
        )
    ].copy()

    search_results = search_results.sort_values(
        by=["lift", "confidence"],
        ascending=False
    )

    if search_results.empty:

        st.warning(
            f"No recommendations found for '{search_product}'."
        )

    else:

        st.success(
            f"Found {len(search_results)} recommendation rules."
        )

        display_results = search_results[
            [
                "antecedent_product_names",
                "consequent_product_names",
                "confidence",
                "lift"
            ]
        ].copy()

        display_results.columns = [
            "Product Purchased",
            "Recommended Product",
            "Confidence",
            "Lift"
        ]

        display_results["Confidence"] = (
            display_results["Confidence"]
            .map("{:.2%}".format)
        )

        display_results["Lift"] = (
            display_results["Lift"]
            .map("{:.2f}".format)
        )

        st.dataframe(
            display_results,
            use_container_width=True,
            hide_index=True
        )


st.divider()


# ============================================================
# TOP ASSOCIATION RULES
# ============================================================

st.subheader("🏆 Top Association Rules")

top_rules = filtered_rules.sort_values(
    by="lift",
    ascending=False
).head(20)


display_top = top_rules[
    [
        "antecedent_product_names",
        "consequent_product_names",
        "confidence",
        "lift"
    ]
].copy()


display_top.columns = [
    "Product A",
    "Product B",
    "Confidence",
    "Lift"
]


display_top["Confidence"] = (
    display_top["Confidence"]
    .map("{:.2%}".format)
)


display_top["Lift"] = (
    display_top["Lift"]
    .map("{:.2f}".format)
)


st.dataframe(
    display_top,
    use_container_width=True,
    hide_index=True
)


st.divider()


# ============================================================
# CHART 1 — TOP RULES BY LIFT
# ============================================================

st.subheader("📈 Top Product Associations by Lift")

chart_data = filtered_rules.sort_values(
    by="lift",
    ascending=False
).head(15).copy()


chart_data["Rule"] = (
    chart_data["antecedent_product_names"]
    + " → "
    + chart_data["consequent_product_names"]
)


fig_lift = px.bar(
    chart_data,
    x="lift",
    y="Rule",
    orientation="h",
    title="Top 15 Association Rules by Lift",
    labels={
        "lift": "Lift",
        "Rule": "Product Association"
    }
)


fig_lift.update_layout(
    yaxis=dict(
        categoryorder="total ascending"
    )
)


st.plotly_chart(
    fig_lift,
    use_container_width=True
)


# ============================================================
# CHART 2 — CONFIDENCE
# ============================================================

st.subheader("📊 Top Product Associations by Confidence")

confidence_data = filtered_rules.sort_values(
    by="confidence",
    ascending=False
).head(15).copy()


confidence_data["Rule"] = (
    confidence_data["antecedent_product_names"]
    + " → "
    + confidence_data["consequent_product_names"]
)


fig_confidence = px.bar(
    confidence_data,
    x="confidence",
    y="Rule",
    orientation="h",
    title="Top 15 Association Rules by Confidence",
    labels={
        "confidence": "Confidence",
        "Rule": "Product Association"
    }
)


fig_confidence.update_layout(
    yaxis=dict(
        categoryorder="total ascending"
    )
)


st.plotly_chart(
    fig_confidence,
    use_container_width=True
)


# ============================================================
# CHART 3 — CONFIDENCE VS LIFT
# ============================================================

st.subheader("🎯 Confidence vs Lift")

scatter_data = filtered_rules.copy()


scatter_data["Rule"] = (
    scatter_data["antecedent_product_names"]
    + " → "
    + scatter_data["consequent_product_names"]
)


fig_scatter = px.scatter(
    scatter_data,
    x="confidence",
    y="lift",
    hover_name="Rule",
    title="Association Rules: Confidence vs Lift",
    labels={
        "confidence": "Confidence",
        "lift": "Lift"
    }
)


st.plotly_chart(
    fig_scatter,
    use_container_width=True
)


# ============================================================
# ALL RULES
# ============================================================

st.subheader("📋 All Association Rules")

all_display = filtered_rules[
    [
        "antecedent_product_names",
        "consequent_product_names",
        "confidence",
        "lift",
        "created_at"
    ]
].copy()


all_display.columns = [
    "Product A",
    "Product B",
    "Confidence",
    "Lift",
    "Created At"
]


st.dataframe(
    all_display,
    use_container_width=True,
    hide_index=True
)


# ============================================================
# FOOTER
# ============================================================

st.divider()

st.caption(
    "Built using Apache Spark FP-Growth, PostgreSQL, Kafka and Streamlit"
)
