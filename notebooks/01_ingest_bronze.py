"""
Notebook Databricks - Etapa 1: Ingestão Bronze

Lê os CSVs brutos do Portal da Transparência (Viagem, Pagamento, Passagem,
Trecho), já extraídos em subpastas por ano dentro do Volume, e grava em
Delta Lake sem transformação, apenas com metadado de ingestão.
"""

from src.ingestion.ingest_bronze import ingest_all

SOURCE_DIR = "/Volumes/govbr/gov_spending/raw_viagens/GOVBR/extraidos"
BRONZE_DIR = "/Volumes/govbr/gov_spending/raw_viagens/bronze"

ingest_all(spark, source_dir=SOURCE_DIR, bronze_dir=BRONZE_DIR)  # noqa: F821 (spark é injetado pelo Databricks)
