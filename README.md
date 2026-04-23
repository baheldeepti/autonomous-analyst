# Autonomous Analyst

LLM agent that investigates anomalies in Brazilian e-commerce data
([Olist dataset on Kaggle](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)).
Built as a live demo for an INFORMS 2026 session on agentic analytics.

**Live demo:** [https://autonomous-analyst.streamlit.app](https://autonomous-analyst.streamlit.app) *(link once deployed)*

## Architecture

- **Detector** — rolling-z-score on STL residuals flags change-points in 20 daily metrics
- **Semantic layer** — YAML-defined metrics, dimensions, and join paths
- **Agent** — Claude with tool use, capped at 6 tool calls per investigation
- **Storage** — DuckDB for source data, telemetry, briefs, and feedback

## Run locally

```bash
git clone https://github.com/YOUR_USERNAME/autonomous-analyst.git
cd autonomous-analyst
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
echo "ANTHROPIC_API_KEY=sk-ant-..." > .env
streamlit run app/Home.py
```

## Notes

- The repo ships with a pre-built `analytics.duckdb`; no CSV download needed
- API key is read from `.env` locally or from Streamlit secrets in production