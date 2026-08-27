"""
Databricks Notebook - Stage 3: Gold Aggregation

Builds the fact table (trip + leg join) and every derived business
metric: spend by agency/year, agency ranking, yearly trend, top
travelers, urgent vs. regular, monthly seasonality, transport mode,
average duration, top destinations, per-capita spend, and outliers.

Writes each table as a MANAGED Unity Catalog table (not a Volume path —
Volumes don't support table LOCATION), ready to query via SQL Warehouse
(e.g. from the Streamlit dashboard).
"""

from src.transformations.gold import run_gold

SILVER_DIR = "/Volumes/govbr/gov_spending/raw_viagens/silver"
CATALOG = "govbr"
SCHEMA = "gov_spending"

run_gold(spark, silver_dir=SILVER_DIR, catalog=CATALOG, schema=SCHEMA)  # noqa: F821
