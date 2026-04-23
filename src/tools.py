"""Tools the agent can call. Every call is validated against metrics.yml."""
import math
import duckdb, yaml
from pathlib import Path
from scipy import stats
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
DB_PATH = ROOT / "analytics.duckdb"
METRICS_PATH = ROOT / "semantic_layer" / "metrics.yml"

with open(METRICS_PATH) as f:
    CFG = yaml.safe_load(f)


def _conn():
    return duckdb.connect(str(DB_PATH), read_only=True)


def _where(start, end):
    return f"AND date BETWEEN DATE '{start}' AND DATE '{end}'"


def _safe_float(x):
    """Convert to float, but return None for NaN/inf (so JSON is valid)."""
    if x is None:
        return None
    try:
        f = float(x)
    except (TypeError, ValueError):
        return None
    if math.isnan(f) or math.isinf(f):
        return None
    return f


def _clean_df(df: pd.DataFrame) -> pd.DataFrame:
    """Replace NaN/inf with None so .to_dict() produces JSON-safe output."""
    return df.where(df.notna(), None)


def query_metric(metric: str, start_date: str, end_date: str) -> dict:
    if metric not in CFG["metrics"]:
        return {"error": f"unknown metric '{metric}'. available: {list(CFG['metrics'])}"}
    sql = CFG["metrics"][metric]["sql"].format(where=_where(start_date, end_date))
    con = _conn()
    df = con.execute(sql).fetchdf()
    con.close()
    return {
        "metric": metric,
        "points": [
            {"date": str(r.date), "value": _safe_float(r.value)}
            for r in df.itertuples()
        ],
        "summary": {
            "mean": _safe_float(df["value"].mean()) if not df.empty else None,
            "min":  _safe_float(df["value"].min())  if not df.empty else None,
            "max":  _safe_float(df["value"].max())  if not df.empty else None,
            "n":    len(df),
        },
    }


def breakdown_by(metric: str, dimension: str, start_date: str,
                 end_date: str, top_n: int = 10) -> dict:
    """Raw counts / totals per dimension value. Use for 'who placed orders?'."""
    if metric not in CFG["metrics"]:
        return {"error": f"unknown metric '{metric}'"}
    mdef = CFG["metrics"][metric]
    if dimension not in mdef["allowed_dimensions"]:
        return {
            "error": f"dim '{dimension}' not allowed for '{metric}'. "
                     f"allowed: {mdef['allowed_dimensions']}"
        }
    dim_col = CFG["dimensions"][dimension]["column"]
    sql = f"""
        SELECT {dim_col} AS dim_value,
               COUNT(DISTINCT order_id) AS orders,
               SUM(item_price)          AS revenue,
               AVG(is_late::DOUBLE)     AS late_rate,
               AVG(review_score)        AS avg_review
        FROM daily_facts
        WHERE date BETWEEN DATE '{start_date}' AND DATE '{end_date}'
          AND {dim_col} IS NOT NULL
        GROUP BY 1
        ORDER BY revenue DESC NULLS LAST
        LIMIT {top_n}
    """
    con = _conn()
    df = con.execute(sql).fetchdf()
    con.close()
    df = _clean_df(df)
    return {
        "metric": metric,
        "dimension": dimension,
        "rows": df.to_dict(orient="records"),
    }


def breakdown_metric_by(metric: str, dimension: str,
                        start_date: str, end_date: str,
                        top_n: int = 10) -> dict:
    """Compute the METRIC ITSELF (not raw counts) split by a dimension.
    e.g. payment_failure_rate per payment_type — answers 'which segment
    is driving the anomaly?'. Enforces a minimum sample size per group."""
    if metric not in CFG["metrics"]:
        return {"error": f"unknown metric '{metric}'"}
    mdef = CFG["metrics"][metric]
    if dimension not in mdef["allowed_dimensions"]:
        return {"error": f"dim '{dimension}' not allowed for '{metric}'. "
                         f"allowed: {mdef['allowed_dimensions']}"}
    dim_col = CFG["dimensions"][dimension]["column"]

    expr_map = {
        "payment_failure_rate":
            "AVG(CASE WHEN order_status IN ('canceled','unavailable') "
            "THEN 1 ELSE 0 END::DOUBLE)",
        "late_delivery_rate": "AVG(is_late::DOUBLE)",
        "avg_review_score":   "AVG(review_score)",
        "avg_order_value":    "AVG(item_price)",
        "gross_revenue":      "SUM(item_price)",
        "order_count":        "COUNT(DISTINCT order_id)",
    }
    if metric not in expr_map:
        return {"error": f"breakdown_metric_by not wired for '{metric}' yet"}

    sql = f"""
        SELECT {dim_col}                AS dim_value,
               {expr_map[metric]}       AS metric_value,
               COUNT(DISTINCT order_id) AS n_orders
        FROM daily_facts
        WHERE date BETWEEN DATE '{start_date}' AND DATE '{end_date}'
          AND {dim_col} IS NOT NULL
        GROUP BY 1
        HAVING n_orders >= 5
        ORDER BY metric_value DESC NULLS LAST
        LIMIT {top_n}
    """
    con = _conn()
    df = con.execute(sql).fetchdf()
    con.close()
    df = _clean_df(df)
    return {"metric": metric, "dimension": dimension,
            "rows": df.to_dict(orient="records")}


def compare_periods(metric: str,
                    window_a_start: str, window_a_end: str,
                    window_b_start: str, window_b_end: str) -> dict:
    a = query_metric(metric, window_a_start, window_a_end)
    b = query_metric(metric, window_b_start, window_b_end)
    if "error" in a:
        return a
    if "error" in b:
        return b
    va = [p["value"] for p in a["points"] if p["value"] is not None]
    vb = [p["value"] for p in b["points"] if p["value"] is not None]
    if not va or not vb:
        return {"error": "empty window"}
    if len(va) > 1 and len(vb) > 1:
        _, p = stats.ttest_ind(va, vb, equal_var=False)
    else:
        p = None
    mean_a = sum(va) / len(va)
    mean_b = sum(vb) / len(vb)
    return {
        "metric": metric,
        "window_a": {"start": window_a_start, "end": window_a_end,
                     "mean": _safe_float(mean_a), "n": len(va)},
        "window_b": {"start": window_b_start, "end": window_b_end,
                     "mean": _safe_float(mean_b), "n": len(vb)},
        "delta_abs": _safe_float(mean_b - mean_a),
        "delta_pct": _safe_float((mean_b - mean_a) / mean_a) if mean_a else None,
        "p_value":   _safe_float(p),
    }


def correlate(metric_a: str, metric_b: str,
              start_date: str, end_date: str) -> dict:
    a = query_metric(metric_a, start_date, end_date)
    b = query_metric(metric_b, start_date, end_date)
    if "error" in a:
        return a
    if "error" in b:
        return b
    da = pd.DataFrame(a["points"]).set_index("date")["value"]
    db = pd.DataFrame(b["points"]).set_index("date")["value"]
    joined = pd.concat([da, db], axis=1, join="inner").dropna()
    if len(joined) < 10:
        return {"error": "not enough overlap"}
    return {
        "metric_a": metric_a,
        "metric_b": metric_b,
        "n_days": len(joined),
        "pearson_r": _safe_float(joined.corr().iloc[0, 1]),
    }


TOOL_SCHEMAS = [
    {
        "name": "query_metric",
        "description": "Daily time series of a metric over a date range.",
        "input_schema": {
            "type": "object",
            "properties": {
                "metric": {
                    "type": "string",
                    "description": f"one of: {list(CFG['metrics'].keys())}",
                },
                "start_date": {"type": "string", "description": "YYYY-MM-DD"},
                "end_date":   {"type": "string", "description": "YYYY-MM-DD"},
            },
            "required": ["metric", "start_date", "end_date"],
        },
    },
    {
        "name": "breakdown_by",
        "description": ("Raw counts / revenue / totals per dimension value "
                        "over a date range. Use when you want to see WHO "
                        "(which states/categories/payment_types) had the "
                        "most activity. For rate metrics like "
                        "payment_failure_rate, prefer breakdown_metric_by."),
        "input_schema": {
            "type": "object",
            "properties": {
                "metric":    {"type": "string"},
                "dimension": {"type": "string",
                              "enum": list(CFG["dimensions"].keys())},
                "start_date": {"type": "string"},
                "end_date":   {"type": "string"},
                "top_n":     {"type": "integer", "default": 10},
            },
            "required": ["metric", "dimension", "start_date", "end_date"],
        },
    },
    {
        "name": "breakdown_metric_by",
        "description": ("Compute the METRIC ITSELF split by a dimension — "
                        "e.g. payment_failure_rate per payment_type. "
                        "Prefer this over breakdown_by when you want to "
                        "compare rate metrics across dimensions and identify "
                        "which segment is driving an anomaly. "
                        "Only returns groups with at least 5 orders."),
        "input_schema": {
            "type": "object",
            "properties": {
                "metric":    {"type": "string"},
                "dimension": {"type": "string",
                              "enum": list(CFG["dimensions"].keys())},
                "start_date": {"type": "string"},
                "end_date":   {"type": "string"},
                "top_n":     {"type": "integer", "default": 10},
            },
            "required": ["metric", "dimension", "start_date", "end_date"],
        },
    },
    {
        "name": "compare_periods",
        "description": "Compare a metric across two date windows with a Welch's t-test.",
        "input_schema": {
            "type": "object",
            "properties": {
                "metric":          {"type": "string"},
                "window_a_start":  {"type": "string"},
                "window_a_end":    {"type": "string"},
                "window_b_start":  {"type": "string"},
                "window_b_end":    {"type": "string"},
            },
            "required": ["metric", "window_a_start", "window_a_end",
                         "window_b_start", "window_b_end"],
        },
    },
    {
        "name": "correlate",
        "description": "Pearson correlation between two metrics over a date range.",
        "input_schema": {
            "type": "object",
            "properties": {
                "metric_a":   {"type": "string"},
                "metric_b":   {"type": "string"},
                "start_date": {"type": "string"},
                "end_date":   {"type": "string"},
            },
            "required": ["metric_a", "metric_b", "start_date", "end_date"],
        },
    },
]

DISPATCH = {
    "query_metric":        query_metric,
    "breakdown_by":        breakdown_by,
    "breakdown_metric_by": breakdown_metric_by,
    "compare_periods":     compare_periods,
    "correlate":           correlate,
}