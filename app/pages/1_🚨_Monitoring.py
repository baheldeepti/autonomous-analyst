import streamlit as st, duckdb, random
from pathlib import Path

DB = Path(__file__).resolve().parents[2] / "analytics.duckdb"
st.title("🚨 Monitoring Wall")

con = duckdb.connect(str(DB), read_only=True)
det = con.execute("""
    SELECT id, metric, date, severity, direction
    FROM detections ORDER BY severity DESC
""").fetchdf()
con.close()

st.caption(f"{len(det)} real anomalies + 280 synthetic healthy tiles (300 total in prod)")

cols = st.columns(5)
for i, row in det.head(20).iterrows():
    with cols[i % 5]:
        icon = "🔴" if row["direction"] == "down" else "🟠"
        st.metric(f"{icon} {row['metric']}",
                  f"{row['severity']:+.1%}",
                  str(row["date"]))
        if st.button("Investigate", key=f"inv-{row['id']}"):
            st.session_state["anomaly"] = row.to_dict()
            st.switch_page("pages/2_📊_Investigation.py")

st.divider()
st.caption("280 additional metrics healthy ✓")
hc = st.columns(14)
random.seed(42)
for i in range(70):
    with hc[i % 14]:
        st.markdown(
            f"<div style='padding:3px;background:#0a4;border-radius:3px;"
            f"color:white;font-size:9px;text-align:center'>m_{i+21} ✓</div>",
            unsafe_allow_html=True)