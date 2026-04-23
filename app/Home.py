import streamlit as st
from pathlib import Path

st.set_page_config(page_title="Autonomous Analyst", page_icon="🤖", layout="wide")

DB = Path(__file__).resolve().parents[1] / "analytics.duckdb"

st.title("🤖 Autonomous Analyst")
st.markdown(
    "A live demo of an LLM agent that investigates anomalies in 100k "
    "real Brazilian e-commerce orders (Olist dataset on Kaggle). "
    "Built for a 15-minute INFORMS talk on agentic analytics."
)

if not DB.exists():
    st.error("Database not found. This should not happen in the deployed app.")
    st.stop()

st.success("System ready. Open **Monitoring** in the sidebar to begin.")

with st.expander("How this works"):
    st.markdown("""
    - **Monitoring page** — change-point detector flags anomalies in 20 business metrics
    - **Investigation page** — click any anomaly; an LLM agent (Claude) runs a bounded tool-calling loop to diagnose the cause
    - **Evaluation page** — feedback log for every investigation

    Architecture: DuckDB + Claude + Streamlit. All tool calls validated
    against a YAML semantic layer before hitting SQL.
    """)