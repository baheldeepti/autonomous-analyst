import streamlit as st, duckdb
from pathlib import Path

DB = Path(__file__).resolve().parents[2] / "analytics.duckdb"
st.title("🔁 Evaluation Harness")

con = duckdb.connect(str(DB), read_only=True)
df = con.execute("""
    SELECT b.investigation_id,
           b.metric,
           b.anomaly_date,
           b.tool_calls,
           b.cost_cents,
           b.headline,
           MAX(b.created_at)        AS created_at,
           COALESCE(SUM(f.thumbs), 0) AS score
    FROM briefs b
    LEFT JOIN feedback f USING (investigation_id)
    GROUP BY b.investigation_id, b.metric, b.anomaly_date,
             b.tool_calls, b.cost_cents, b.headline
    ORDER BY created_at DESC
    LIMIT 50
""").fetchdf()
con.close()

if df.empty:
    st.info("Run an investigation first from the Monitoring page.")
else:
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Investigations", len(df))
    c2.metric("Avg tool calls", f"{df['tool_calls'].mean():.1f}")
    c3.metric("Avg cost",       f"{df['cost_cents'].mean():.2f}¢")
    c4.metric("Net thumbs",     int(df["score"].sum()))

    # reorder columns for display
    show = df[["created_at", "metric", "anomaly_date",
               "tool_calls", "cost_cents", "score", "headline",
               "investigation_id"]]
    st.dataframe(show, use_container_width=True, hide_index=True)