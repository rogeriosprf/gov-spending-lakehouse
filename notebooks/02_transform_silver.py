"""
Databricks Notebook - Stage 2: Silver Transformation

Reads each Bronze table, applies typing (dates, currency values,
booleans) and removes exact duplicates. No join between tables — each
one stays at its original granularity. Writes to Delta Lake.
"""

from src.transformations.silver import run_silver

BRONZE_DIR = "/Volumes/govbr/gov_spending/raw_viagens/bronze"
SILVER_DIR = "/Volumes/govbr/gov_spending/raw_viagens/silver"

run_silver(spark, bronze_dir=BRONZE_DIR, silver_dir=SILVER_DIR)  # noqa: F821
