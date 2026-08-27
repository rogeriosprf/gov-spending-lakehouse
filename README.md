# Gov Spending Lakehouse

A data pipeline built with the Medallion architecture (Bronze/Silver/Gold)
over Brazilian federal government business travel spending data
(Portal da Transparência), ~9.7M records.

Portfolio project built to demonstrate hands-on experience with a modern
cloud data engineering stack: **PySpark, Delta Lake, Databricks, and
Airflow**.

## Stack

- **Processing**: PySpark
- **Storage**: Delta Lake (ACID format on top of Parquet)
- **Environment**: Databricks (Free Edition, Unity Catalog)
- **Orchestration**: Apache Airflow
- **Data source**: Portal da Transparência (Brazilian federal government)
- **Dashboard**: Streamlit, connected live to a Databricks SQL Warehouse

## Architecture

```
Source (Portal da Transparência)
        │
        ▼
   ┌─────────┐      ┌─────────┐      ┌─────────┐
   │ BRONZE  │ ───► │ SILVER  │ ───► │  GOLD   │
   │  (raw)  │      │(cleaned)│      │(aggreg.)│
   └─────────┘      └─────────┘      └─────────┘
        orchestrated by Airflow (dags/)
                                          │
                                          ▼
                              Streamlit dashboard
                          (live query via Databricks SQL)
```

Full details of each layer in [docs/architecture.md](docs/architecture.md).

## Repository structure

```
gov-spending-lakehouse/
├── data/                   # not versioned (see .gitignore) — local test layers
│   ├── bronze/
│   ├── silver/
│   └── gold/
├── notebooks/              # Databricks notebooks (one per pipeline stage)
├── dags/                   # Airflow DAGs
├── src/
│   ├── ingestion/          # source collection scripts
│   ├── transformations/    # Bronze→Silver→Gold transformation logic
│   └── utils/              # shared helpers (BR date/decimal parsing, column sanitization)
├── app/                    # Streamlit dashboard
│   ├── streamlit_app.py
│   └── db_connection.py
├── .streamlit/
│   └── secrets.toml.example
├── docs/
│   └── architecture.md
└── tests/
```

## Pipeline results

16 years of data (2011–2026), ~9.7M business trips processed end to end.

| Layer | Highlights |
|---|---|
| Bronze | 4 raw tables (trip, payment, ticket, leg), ingested as-is with lineage metadata |
| Silver | Typed dates/currency/booleans; **648K duplicate payment records found and removed** (~4% of the table) |
| Gold | 12 business-ready metric tables (spend by agency/year, top travelers, seasonality, outliers, etc.) built from a single fact table, joined once |

## Dashboard

A Streamlit dashboard connects **live** to the Databricks SQL Warehouse
(via `databricks-sql-connector`) and queries the Gold tables directly
from Unity Catalog — no manual file export.

Setup:

1. Copy `.streamlit/secrets.toml.example` to `.streamlit/secrets.toml`
   and fill in your SQL Warehouse credentials (instructions inside the
   example file itself). This file is never committed.
2. Install dependencies and run:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
streamlit run app/streamlit_app.py
```

Note: `requirements.txt` (repo root) holds the dashboard's dependencies —
Streamlit Community Cloud always looks for this exact file name/path
when deploying. Pipeline dependencies (PySpark, Delta Lake, Airflow) are
listed separately in `requirements-pipeline.txt`, for reference only —
they're already available natively inside Databricks notebooks and
aren't needed to run the dashboard.

Gold tables must already be registered in Unity Catalog (done
automatically by the pipeline, in `run_gold()`, as
`govbr.gov_spending.gold_<table_name>`).

## Status

✅ Full pipeline: Bronze → Silver → Gold (13 tables) → live dashboard.

See [docs/architecture.md](docs/architecture.md) for scope decisions and
what's intentionally out of scope for now (Terraform, native AWS,
Airflow running in production).

## Author

Paulo Rogério — [portfolio](https://rogeriosprf.github.io/portifolio/)
