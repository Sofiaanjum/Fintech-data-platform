import streamlit as st
import snowflake.connector
import pandas as pd
import plotly.express as px
import os
from dotenv import load_dotenv

load_dotenv()

st.set_page_config(
    page_title="Fintech Fraud Dashboard",
    page_icon="🏦",
    layout="wide"
)

@st.cache_resource
def get_connection():
    return snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database="FINTECH_DEV_DB",
        schema="MARTS",
        role="DATA_ENG_ROLE"
    )

@st.cache_data(ttl=300)
def load_data():
    conn = get_connection()
    query = "SELECT * FROM DIM_TRANSACTIONS_SUMMARY"
    df = pd.read_sql(query, conn)
    df.columns = [c.lower() for c in df.columns]
    return df

df = load_data()

st.title("Fintech Transaction & Fraud Analytics")
st.caption("Source: IEEE-CIS Fraud Detection Dataset -- Live via Kafka -> Snowflake -> dbt")

# KPI row
col1, col2, col3, col4 = st.columns(4)

total_txn    = df["total_transactions"].sum()
total_fraud  = df["total_fraud"].sum()
fraud_rate   = round((total_fraud / total_txn) * 100, 2)
total_volume = round(df["total_volume"].sum(), 2)

col1.metric("Total Transactions", f"{total_txn:,}")
col2.metric("Total Fraud Cases", f"{int(total_fraud):,}")
col3.metric("Fraud Rate", f"{fraud_rate}%")
col4.metric("Total Volume", f"${total_volume:,.2f}")

st.divider()

# Row 2 -- charts
col1, col2 = st.columns(2)

with col1:
    st.subheader("Fraud by Card Network")
    fraud_by_network = (
        df.groupby("card_network")
        .agg(total_fraud=("total_fraud", "sum"),
             total_transactions=("total_transactions", "sum"))
        .reset_index()
    )
    fraud_by_network["fraud_rate"] = round(
        fraud_by_network["total_fraud"] / fraud_by_network["total_transactions"] * 100, 2
    )
    fig = px.bar(
        fraud_by_network,
        x="card_network",
        y="fraud_rate",
        color="card_network",
        labels={"fraud_rate": "Fraud Rate (%)", "card_network": "Card Network"},
    )
    st.plotly_chart(fig, use_container_width=True)

with col2:
    st.subheader("Transaction Volume by Time of Day")
    vol_by_time = (
        df.groupby("time_of_day")
        .agg(total_volume=("total_volume", "sum"))
        .reset_index()
    )
    fig2 = px.pie(
        vol_by_time,
        names="time_of_day",
        values="total_volume",
        hole=0.4,
    )
    st.plotly_chart(fig2, use_container_width=True)

st.divider()

# Row 3
col1, col2 = st.columns(2)

with col1:
    st.subheader("Risk Level Distribution")
    risk_dist = (
        df.groupby("risk_level")
        .agg(total_transactions=("total_transactions", "sum"))
        .reset_index()
    )
    fig3 = px.bar(
        risk_dist,
        x="risk_level",
        y="total_transactions",
        color="risk_level",
        labels={"total_transactions": "Transactions", "risk_level": "Risk Level"},
    )
    st.plotly_chart(fig3, use_container_width=True)

with col2:
    st.subheader("Avg Transaction Amount by Size")
    amt_by_size = (
        df.groupby("transaction_size")
        .agg(avg_amt=("avg_transaction_amt", "mean"))
        .reset_index()
    )
    fig4 = px.bar(
        amt_by_size,
        x="transaction_size",
        y="avg_amt",
        labels={"avg_amt": "Avg Amount ($)", "transaction_size": "Transaction Size"},
    )
    st.plotly_chart(fig4, use_container_width=True)

st.divider()
st.subheader("Raw Summary Table")
st.dataframe(df, use_container_width=True)