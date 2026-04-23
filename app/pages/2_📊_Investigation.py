import sys, streamlit as st, duckdb
import pandas as pd, plotly.express as px
from pathlib import Path
from datetime import datetime, timedelta

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from src.agent import investigate
from src.tools import query_metric

DB = Path(__file__).resolve().parents[2] / "analytics.duckdb"
st.title("📊 Active Investigation")

anomaly = st.session_state.get("anomaly")
if not anomaly:
    st.warning("Pick an anomaly from Monitoring first.")
    st.stop()

st.subheader(f"{anomaly['metric']} on {anomaly['date']}")
st.write(f"**Direction:** {anomaly['direction']}  |  **Severity:** {anomaly['severity']:+.1%}")

left, right = st.columns([1, 1])

with left:
    d = datetime.fromisoformat(str(anomaly["date"]))
    data = query_metric(anomaly["metric"],
                        (d - timedelta(days=60)).date().isoformat(),
                        (d + timedelta(days=14)).date().isoformat())
    df = pd.DataFrame(data["points"])
    if not df.empty:
        df["date"] = pd.to_datetime(df["date"])
        fig = px.line(df, x="date", y="value", title=anomaly["metric"])
        fig.add_vline(x=d, line_dash="dash", line_color="red")
        st.plotly_chart(fig, use_container_width=True)

with right:
    st.markdown("### Agent reasoning")
    log_area = st.empty()
    buffer = {"t": ""}

    def cb(chunk):
        buffer["t"] += chunk
        log_area.code(buffer["t"], language=None)

    if st.button("▶ Run investigation", type="primary"):
        with st.spinner("thinking…"):
            result = investigate(anomaly, stream_cb=cb)
        st.session_state["result"] = result

if "result" in st.session_state:
    r = st.session_state["result"]
    st.markdown("---")
    st.markdown("### 📝 Brief")
    st.markdown(
        f"<div style='background:#f4f4f8;padding:14px;border-radius:8px;"
        f"border-left:4px solid #4a4;font-family:ui-monospace,monospace;"
        f"white-space:pre-wrap'>{r['brief']}</div>",
        unsafe_allow_html=True)
    st.caption(f"{r['tool_calls']} tool calls · {r['cost_cents']:.2f}¢ · id={r['investigation_id']}")

    c1, c2 = st.columns(2)
    if c1.button("👍 Useful"):
        con = duckdb.connect(str(DB))
        con.execute("INSERT INTO feedback VALUES (?,1,'',CURRENT_TIMESTAMP)",
                    [r["investigation_id"]])
        con.close()
        st.success("logged")
    if c2.button("👎 Not useful"):
        con = duckdb.connect(str(DB))
        con.execute("INSERT INTO feedback VALUES (?,-1,'',CURRENT_TIMESTAMP)",
                    [r["investigation_id"]])
        con.close()
        st.success("logged")