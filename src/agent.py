"""Planner → tools → verifier → final brief. 6-call budget."""
import os
import json
import time
import uuid
from pathlib import Path

import duckdb
from dotenv import load_dotenv
from anthropic import Anthropic

from .tools import TOOL_SCHEMAS, DISPATCH


# ---------------------------------------------------------------------------
# API key loading
# ---------------------------------------------------------------------------
# Local dev: read from .env
# Streamlit Cloud: read from st.secrets (no .env exists there)
load_dotenv()

if not os.getenv("ANTHROPIC_API_KEY"):
    try:
        import streamlit as st
        if "ANTHROPIC_API_KEY" in st.secrets:
            os.environ["ANTHROPIC_API_KEY"] = st.secrets["ANTHROPIC_API_KEY"]
    except Exception:
        # streamlit not installed (e.g. pure-CLI use) → fine, just skip
        pass

# Fail fast with a readable error instead of a cryptic TypeError from the SDK.
if not os.getenv("ANTHROPIC_API_KEY"):
    raise RuntimeError(
        "ANTHROPIC_API_KEY is not set.\n"
        "  Locally:        add it to the project's .env file.\n"
        "  Streamlit Cloud: Settings → Secrets → "
        "ANTHROPIC_API_KEY = \"sk-ant-...\""
    )


# ---------------------------------------------------------------------------
# Paths and client
# ---------------------------------------------------------------------------
ROOT = Path(__file__).resolve().parents[1]
DB_PATH = ROOT / "analytics.duckdb"

client = Anthropic()
MODEL = "claude-sonnet-4-5"
MAX_CALLS = 6


# ---------------------------------------------------------------------------
# System prompt
# ---------------------------------------------------------------------------
SYSTEM_PROMPT = """You are an autonomous analyst investigating a flagged anomaly.

Rules:
1. Form 2-3 concrete hypotheses before calling tools.
2. Prefer breakdown_by and compare_periods over raw query_metric.
3. Strict budget: 6 tool calls total. Choose carefully.
4. After each tool result, briefly note whether it supports or contradicts your hypotheses.
5. When you have enough evidence (usually 3-5 calls), stop and produce the final brief.

Final brief format — plain text, no markdown:
HEADLINE: one specific, quantified sentence.
EVIDENCE:
- bullet with a number
- bullet with a number
- bullet with a number
CONFIDENCE: low | medium | high — one phrase justification.
PROPOSED ACTION: one concrete next step.
"""


# ---------------------------------------------------------------------------
# Persistence helpers (short-lived DuckDB connections to avoid write locks)
# ---------------------------------------------------------------------------
def _log_telemetry(inv, step, role, tool, inp, out, tin, tout, lat):
    """Log one agent or tool step. Short-lived write connection."""
    cost = (tin * 3 / 1_000_000 + tout * 15 / 1_000_000) * 100
    con = duckdb.connect(str(DB_PATH))
    try:
        con.execute(
            """
            INSERT INTO telemetry
            VALUES (?,?,?,?,?,?,?,?,?,?,CURRENT_TIMESTAMP)
            """,
            [
                inv, step, role, tool,
                json.dumps(inp, default=str)[:4000],
                json.dumps(out, default=str)[:4000],
                tin, tout, lat, cost,
            ],
        )
    finally:
        con.close()


def _save_brief(inv, metric, anomaly_date, text, calls, cost):
    con = duckdb.connect(str(DB_PATH))
    try:
        con.execute(
            """
            INSERT INTO briefs
            VALUES (?, ?, ?, ?, ?, NULL, ?, ?, CURRENT_TIMESTAMP)
            """,
            [
                inv, metric, anomaly_date,
                text.split("\n")[0][:200] if text else "no brief",
                text, calls, cost,
            ],
        )
    finally:
        con.close()


# ---------------------------------------------------------------------------
# Main investigation loop
# ---------------------------------------------------------------------------
def investigate(anomaly: dict, stream_cb=None):
    inv = str(uuid.uuid4())[:8]

    user = (
        f"Anomaly flagged:\n"
        f"  metric: {anomaly['metric']}\n"
        f"  date: {anomaly['date']}\n"
        f"  direction: {anomaly['direction']}\n"
        f"  relative change: {anomaly['severity']:.1%}\n\n"
        f"Investigate why. Budget: {MAX_CALLS} tool calls."
    )
    messages = [{"role": "user", "content": user}]

    calls = 0
    step = 0
    total_cost = 0.0
    final_text = ""

    while True:
        step += 1
        t0 = time.time()
        resp = client.messages.create(
            model=MODEL,
            max_tokens=1500,
            system=SYSTEM_PROMPT,
            tools=TOOL_SCHEMAS,
            messages=messages,
        )
        lat = int((time.time() - t0) * 1000)
        tin = resp.usage.input_tokens
        tout = resp.usage.output_tokens
        total_cost += (tin * 3 / 1_000_000 + tout * 15 / 1_000_000) * 100

        text_chunks = [b.text for b in resp.content if b.type == "text"]
        tool_uses = [b for b in resp.content if b.type == "tool_use"]
        text_out = "".join(text_chunks)

        if stream_cb and text_out:
            stream_cb(text_out + "\n")

        _log_telemetry(
            inv, step, "assistant", None,
            {"preview": text_out[:200]},
            {"n_tools": len(tool_uses)},
            tin, tout, lat,
        )

        if resp.stop_reason == "end_turn" or not tool_uses:
            final_text = text_out
            break

        messages.append({"role": "assistant", "content": resp.content})
        tool_results = []
        for tu in tool_uses:
            calls += 1
            if calls > MAX_CALLS:
                tool_results.append({
                    "type": "tool_result",
                    "tool_use_id": tu.id,
                    "content": "Budget exceeded. Produce final brief now.",
                })
                continue

            if stream_cb:
                stream_cb(f"→ {tu.name}({json.dumps(dict(tu.input), default=str)})\n")

            t0 = time.time()
            try:
                result = DISPATCH[tu.name](**tu.input)
            except Exception as e:
                result = {"error": f"{type(e).__name__}: {e}"}

            if stream_cb:
                preview = json.dumps(result, default=str)[:300]
                stream_cb(f"   ↳ {preview}\n")

            _log_telemetry(
                inv, step, "tool", tu.name,
                dict(tu.input), result,
                0, 0, int((time.time() - t0) * 1000),
            )
            tool_results.append({
                "type": "tool_result",
                "tool_use_id": tu.id,
                "content": json.dumps(result, default=str)[:4000],
            })
        messages.append({"role": "user", "content": tool_results})

    _save_brief(
        inv, anomaly["metric"], anomaly["date"],
        final_text, calls, total_cost,
    )
    return {
        "investigation_id": inv,
        "brief": final_text,
        "tool_calls": calls,
        "cost_cents": total_cost,
    }


# ---------------------------------------------------------------------------
# Command-line runner (pick the highest-severity real anomaly and investigate)
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    con = duckdb.connect(str(DB_PATH), read_only=True)
    row = con.execute("""
        SELECT metric, date, severity, direction
        FROM detections
        WHERE metric IN ('late_delivery_rate','payment_failure_rate','avg_order_value')
        ORDER BY severity DESC LIMIT 1
    """).fetchone()
    con.close()

    if not row:
        print("No detections — run `python -m src.detector` first.")
    else:
        a = {
            "metric": row[0], "date": str(row[1]),
            "severity": row[2], "direction": row[3],
        }
        print(f"Investigating: {a}\n")
        out = investigate(a, stream_cb=lambda s: print(s, end="", flush=True))
        print(f"\n\n=== {out['tool_calls']} calls · {out['cost_cents']:.2f}¢ ===")
