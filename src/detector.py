"""Pure-Python change-point detection — no compilation required."""
import duckdb, yaml
import numpy as np
import pandas as pd
from pathlib import Path
from statsmodels.tsa.seasonal import STL

ROOT = Path(__file__).resolve().parents[1]
DB_PATH = ROOT / "analytics.duckdb"
METRICS_PATH = ROOT / "semantic_layer" / "metrics.yml"

# Drop detections before this date — Olist's first months are too sparse
# to produce meaningful anomalies (single-digit orders per day).
MIN_ANOMALY_DATE = pd.Timestamp("2017-02-01").date()

# Cap severity: anything above this is almost certainly a sparsity artifact,
# not a real business anomaly worth showing on the monitoring wall.
MAX_SEVERITY = 5.0   # i.e. 500%


def load_metrics():
    with open(METRICS_PATH) as f:
        return yaml.safe_load(f)


def fetch_series(con, mdef):
    sql = mdef["sql"].format(where="")
    df = con.execute(sql).fetchdf()
    if df.empty:
        return df
    df["date"] = pd.to_datetime(df["date"])
    df = df.set_index("date").asfreq("D").fillna(0)
    return df


def rolling_zscore_changepoints(series, window=14, z_thresh=3.0, min_gap=14):
    """Flag points whose value deviates > z_thresh rolling SDs from the mean.
    Same role as PELT for our demo — simpler, pure Python, no C build."""
    vals = series.values.astype(float)
    n = len(vals)
    if n < 2 * window:
        return []
    rolling = pd.Series(vals).rolling(window, min_periods=window)
    mean = rolling.mean().values
    std = rolling.std().values
    cps, last = [], -min_gap
    for i in range(window, n):
        if std[i] and not np.isnan(std[i]) and std[i] > 0:
            z = abs(vals[i] - mean[i]) / std[i]
            if z > z_thresh and i - last >= min_gap:
                cps.append(i)
                last = i
    return cps


def score(series, idx):
    if idx < 7 or idx > len(series) - 7:
        return 0.0, "flat"
    before = series.iloc[max(0, idx - 14):idx].mean()
    after = series.iloc[idx:idx + 7].mean()
    if before == 0:
        return 0.0, "flat"
    rel = (after - before) / abs(before)
    return abs(rel), ("up" if rel > 0 else "down")


def run():
    con = duckdb.connect(str(DB_PATH))
    cfg = load_metrics()
    con.execute("DELETE FROM detections")
    row_id = 1
    for name, mdef in cfg["metrics"].items():
        df = fetch_series(con, mdef)
        if df.empty or len(df) < 60:
            continue
        try:
            stl = STL(df["value"], period=7, robust=True).fit()
            residual = stl.resid.fillna(0)
        except Exception:
            residual = df["value"]

        for idx in rolling_zscore_changepoints(residual, window=14, z_thresh=3.0):
            sev, direction = score(df["value"], idx)
            if sev < 0.15:
                continue
            if sev > MAX_SEVERITY:          # drop sparsity artifacts
                continue
            d = df.index[idx].date()
            if d < MIN_ANOMALY_DATE:        # drop early-history noise
                continue
            con.execute("""
                INSERT INTO detections
                VALUES (?, ?, NULL, NULL, ?, ?, ?, CURRENT_TIMESTAMP)
            """, [row_id, name, d, float(sev), direction])
            row_id += 1
            print(f"  {name:22s} {d}  Δ={sev:+.1%}  ({direction})")

    total = con.execute("SELECT COUNT(*) FROM detections").fetchone()[0]
    print(f"\n✓ {total} anomalies written")
    con.close()


if __name__ == "__main__":
    run()