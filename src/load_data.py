"""One-time loader: Olist CSVs → DuckDB with a daily_facts view."""
import duckdb
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data" / "olist"
DB_PATH = ROOT / "analytics.duckdb"


def load():
    con = duckdb.connect(str(DB_PATH))

    tables = {
        "orders": "olist_orders_dataset.csv",
        "order_items": "olist_order_items_dataset.csv",
        "payments": "olist_order_payments_dataset.csv",
        "reviews": "olist_order_reviews_dataset.csv",
        "customers": "olist_customers_dataset.csv",
        "products": "olist_products_dataset.csv",
        "sellers": "olist_sellers_dataset.csv",
        "category_translation": "product_category_name_translation.csv",
    }
    for name, fname in tables.items():
        path = DATA_DIR / fname
        con.execute(f"CREATE OR REPLACE TABLE {name} AS SELECT * FROM read_csv_auto('{path}')")
        n = con.execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0]
        print(f"  loaded {name:22s} {n:>8} rows")

    con.execute("""
        CREATE OR REPLACE VIEW daily_facts AS
        SELECT
            DATE_TRUNC('day', o.order_purchase_timestamp)::DATE AS date,
            c.customer_state                                    AS state,
            COALESCE(ct.product_category_name_english,
                     p.product_category_name, 'unknown')        AS category,
            pay.payment_type                                    AS payment_type,
            o.order_status                                      AS order_status,
            o.order_id,
            oi.price                                            AS item_price,
            oi.freight_value                                    AS freight,
            pay.payment_value                                   AS payment_value,
            r.review_score                                      AS review_score,
            CASE
              WHEN o.order_delivered_customer_date IS NOT NULL
               AND o.order_estimated_delivery_date IS NOT NULL
               AND o.order_delivered_customer_date > o.order_estimated_delivery_date
              THEN 1 ELSE 0
            END AS is_late
        FROM orders o
        LEFT JOIN order_items oi ON o.order_id = oi.order_id
        LEFT JOIN payments    pay ON o.order_id = pay.order_id
        LEFT JOIN reviews     r   ON o.order_id = r.order_id
        LEFT JOIN customers   c   ON o.customer_id = c.customer_id
        LEFT JOIN products    p   ON oi.product_id = p.product_id
        LEFT JOIN category_translation ct
               ON p.product_category_name = ct.product_category_name
        WHERE o.order_purchase_timestamp IS NOT NULL
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS detections (
            id BIGINT, metric VARCHAR, dim VARCHAR, dim_value VARCHAR,
            date DATE, severity DOUBLE, direction VARCHAR,
            detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS telemetry (
            investigation_id VARCHAR, step INTEGER, role VARCHAR,
            tool VARCHAR, input_json VARCHAR, output_json VARCHAR,
            tokens_in INTEGER, tokens_out INTEGER,
            latency_ms INTEGER, cost_cents DOUBLE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS feedback (
            investigation_id VARCHAR, thumbs INTEGER,
            note VARCHAR, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS briefs (
            investigation_id VARCHAR PRIMARY KEY,
            metric VARCHAR, anomaly_date DATE,
            headline VARCHAR, body VARCHAR, confidence DOUBLE,
            tool_calls INTEGER, cost_cents DOUBLE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    print(f"\n✓ analytics.duckdb ready at {DB_PATH}")
    con.close()


if __name__ == "__main__":
    load()