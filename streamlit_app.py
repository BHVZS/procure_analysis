import streamlit as st
import pandas as pd
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark import Session

# ---------------------------------
# PAGE CONFIG
# ---------------------------------
st.set_page_config(
    page_title="Procurement Analysis",
    layout="wide"
)

st.title("📊 Procurement Analysis Dashboard")

# ---------------------------------
# SNOWFLAKE SESSION (Hybrid Mode)
# Works in BOTH:
# 1) Snowflake Streamlit
# 2) Streamlit Cloud
# ---------------------------------
@st.cache_resource
def create_session():
    try:
        # If running inside Snowflake
        return get_active_session()
    except:
        # If running on Streamlit Cloud
        connection_parameters = {
            "account": st.secrets["snowflake"]["account"],
            "user": st.secrets["snowflake"]["user"],
            "password": st.secrets["snowflake"]["password"],
            "role": st.secrets["snowflake"]["role"],
            "warehouse": st.secrets["snowflake"]["warehouse"],
            "database": st.secrets["snowflake"]["database"],
            "schema": st.secrets["snowflake"]["schema"]
        }
        return Session.builder.configs(connection_parameters).create()

session = create_session()

# ---------------------------------
# KPI SECTION
# ---------------------------------
kpi_df = session.sql("""
SELECT
    total_spend,
    total_transactions,
    total_quantity
FROM EXECUTIVE_KPI
""").to_pandas()

c1, c2, c3 = st.columns(3)

c1.metric("💰 Total Spend", f"{int(kpi_df['TOTAL_SPEND'][0]):,}")
c2.metric("📦 Transactions", int(kpi_df['TOTAL_TRANSACTIONS'][0]))
c3.metric("📊 Total Quantity", int(kpi_df['TOTAL_QUANTITY'][0]))

# ---------------------------------
# MONTHLY PROCUREMENT TREND
# ---------------------------------
st.subheader("📈 Monthly Procurement Trend")

monthly_df = session.sql("""
SELECT
    year,
    month,
    total_spend
FROM MONTHLY_PROCUREMENT_TREND
ORDER BY year, month
""").to_pandas()

# Create proper DATE column
monthly_df["DATE"] = pd.to_datetime(
    monthly_df["YEAR"].astype(str) + "-" +
    monthly_df["MONTH"].astype(str) + "-01"
)

st.line_chart(
    monthly_df.set_index("DATE")["TOTAL_SPEND"]
)

# ---------------------------------
# SPEND BY CATEGORY
# ---------------------------------
st.subheader("📊 Spend by Category")

category_df = session.sql("""
SELECT
    category,
    total_invoice_spend
FROM CATEGORY_ANALYSIS
ORDER BY total_invoice_spend DESC
""").to_pandas()

st.bar_chart(
    category_df.set_index("CATEGORY")["TOTAL_INVOICE_SPEND"]
)

# ---------------------------------
# SPEND BY CITY
# ---------------------------------
st.subheader("🏙️ Spend by City")

city_df = session.sql("""
SELECT
    city,
    total_spend
FROM CITY_ANALYSIS
ORDER BY total_spend DESC
""").to_pandas()

st.bar_chart(
    city_df.set_index("CITY")["TOTAL_SPEND"]
)
