"""
Databricks Notebook - Stage 1: Bronze Ingestion

Reads the raw Portal da Transparência CSVs (trip, payment, ticket, leg),
already extracted into per-year subfolders inside the Volume, and writes
them to Delta Lake with no transformation, only ingestion metadata.
"""

from src.ingestion.ingest_bronze import ingest_all

SOURCE_DIR = "/Volumes/govbr/gov_spending/raw_viagens/GOVBR/extraidos"
BRONZE_DIR = "/Volumes/govbr/gov_spending/raw_viagens/bronze"

ingest_all(spark, source_dir=SOURCE_DIR, bronze_dir=BRONZE_DIR)  # noqa: F821 (spark is injected by Databricks)
