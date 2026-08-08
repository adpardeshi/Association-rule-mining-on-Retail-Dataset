import streamlit as st
import pandas as pd
import plotly.express as px
from pathlib import Path

# -------------------------------------------------------
# Page Configuration
# -------------------------------------------------------
st.set_page_config(
    page_title="Instacart Market Basket Analysis",
    page_icon="🛒",
    layout="wide"
)

# -------------------------------------------------------
# Locate Project Folder
# -------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent.parent

# -------------------------------------------------------
# Load Data
# -------------------------------------------------------
itemsets = pd.read_parquet(BASE_DIR / "data" / "frequent_itemsets")
rules = pd.read_parquet(BASE_DIR / "data" / "association_rules")
products = pd.read_csv(BASE_DIR / "data" / "products.csv")

# -------------------------------------------------------
# Product Dictionary
# -------------------------------------------------------
product_map = dict(zip(products.product_id, products.product_name))


def convert_items(items):
    names = []
    for pid in items:
        names.append(product_map.get(pid, str(pid)))
    return ", ".join(names)


# -------------------------------------------------------
# Convert IDs to Product Names
# -------------------------------------------------------
itemsets["Products"] = itemsets["items"].apply(convert_items)

rules["Antecedent"] = rules["antecedent"].apply(convert_items)
rules["Consequent"] = rules["consequent"].apply(convert_items)

# -------------------------------------------------------
# Sidebar
# -------------------------------------------------------
st.sidebar.title("🛒 Dashboard")

st.sidebar.markdown("## Project")

st.sidebar.write("Market Basket Analysis")

st.sidebar.write("Algorithm : FP-Growth")

st.sidebar.write("Dataset : Instacart")

st.sidebar.markdown("---")

st.sidebar.metric("Transactions", "3,214,874")

st.sidebar.metric("Frequent Itemsets", len(itemsets))

st.sidebar.metric("Association Rules", len(rules))

# -------------------------------------------------------
# Main Title
# -------------------------------------------------------
st.title("🛒 Instacart Market Basket Analysis Dashboard")

st.markdown(
    """
Interactive dashboard generated using **Apache Spark FP-Growth**
on the Instacart dataset.
"""
)

# -------------------------------------------------------
# KPIs
# -------------------------------------------------------
c1, c2, c3 = st.columns(3)

c1.metric("Transactions", "3,214,874")
c2.metric("Frequent Itemsets", len(itemsets))
c3.metric("Association Rules", len(rules))

st.divider()

# -------------------------------------------------------
# Top Frequent Products
# -------------------------------------------------------
st.header("📊 Top 20 Frequent Itemsets")

top20 = (
    itemsets.sort_values("freq", ascending=False)
    .head(20)
    .copy()
)

fig = px.bar(
    top20,
    x="Products",
    y="freq",
    color="freq",
    text="freq",
    title="Top 20 Most Frequent Itemsets"
)

fig.update_layout(
    xaxis_title="Products",
    yaxis_title="Frequency",
    height=550
)

st.plotly_chart(fig, use_container_width=True)

# -------------------------------------------------------
# Association Rules Chart
# -------------------------------------------------------
st.divider()

st.header("📈 Association Rules")

rules["Rule"] = (
    rules["Antecedent"] +
    " ➜ " +
    rules["Consequent"]
)

fig2 = px.scatter(
    rules,
    x="support",
    y="confidence",
    color="lift",
    size="lift",
    hover_name="Rule",
    text="Rule",
    title="Support vs Confidence"
)

fig2.update_traces(textposition="top center")

fig2.update_layout(height=600)

st.plotly_chart(fig2, use_container_width=True)

# -------------------------------------------------------
# Frequent Itemsets Table
# -------------------------------------------------------
st.divider()

st.header("📋 Frequent Itemsets")

display_itemsets = (
    itemsets[["Products", "freq"]]
    .sort_values("freq", ascending=False)
)

st.dataframe(
    display_itemsets,
    use_container_width=True,
    hide_index=True,
    height=450
)

# -------------------------------------------------------
# Association Rules Table
# -------------------------------------------------------
st.divider()

st.header("📋 Association Rules")

display_rules = rules[
    [
        "Antecedent",
        "Consequent",
        "confidence",
        "lift",
        "support"
    ]
].sort_values("lift", ascending=False)

st.dataframe(
    display_rules,
    use_container_width=True,
    hide_index=True,
    height=250
)

# -------------------------------------------------------
# Top 10 Frequent Itemsets
# -------------------------------------------------------
st.divider()

st.header("🏆 Top 10 Itemsets")

st.table(display_itemsets.head(10))

# -------------------------------------------------------
# Rule Statistics
# -------------------------------------------------------
st.divider()

st.header("📌 Rule Statistics")

st.write("Average Confidence :", round(rules["confidence"].mean(), 3))
st.write("Average Lift :", round(rules["lift"].mean(), 3))
st.write("Average Support :", round(rules["support"].mean(), 3))

# -------------------------------------------------------
# Footer
# -------------------------------------------------------
st.divider()

st.caption(
    "Developed using Apache Spark • Docker • Streamlit • FP-Growth"
)