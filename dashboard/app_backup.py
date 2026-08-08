import streamlit as st
import pandas as pd
from pathlib import Path

# ----------------------------
# Page Configuration
# ----------------------------
st.set_page_config(
    page_title="Instacart Market Basket Analysis",
    layout="wide"
)

st.title("🛒 Instacart Market Basket Analysis Dashboard")

# ----------------------------
# Load Data
# ----------------------------
BASE_DIR = Path(__file__).resolve().parent.parent

itemsets_path = BASE_DIR / "data" / "frequent_itemsets"
rules_path = BASE_DIR / "data" / "association_rules"

itemsets = pd.read_parquet(itemsets_path)
rules = pd.read_parquet(rules_path)

# ----------------------------
# KPI Cards
# ----------------------------
col1, col2, col3 = st.columns(3)

with col1:
    st.metric("Transactions", "3,214,874")

with col2:
    st.metric("Frequent Itemsets", len(itemsets))

with col3:
    st.metric("Association Rules", len(rules))

st.divider()

# ----------------------------
# Frequent Itemsets
# ----------------------------
st.header("Frequent Itemsets")

st.dataframe(
    itemsets,
    use_container_width=True,
    hide_index=True
)

st.divider()

# ----------------------------
# Association Rules
# ----------------------------
st.header("Association Rules")

st.dataframe(
    rules,
    use_container_width=True,
    hide_index=True
)