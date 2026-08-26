"""
Notebook Databricks - Etapa 4: Exportação da Gold para o dashboard

Exporta as tabelas Gold agregadas (não a fato_viagem, que tem 9.7M de
linhas e não é consumida diretamente pelo dashboard) como CSV, prontas
para download e uso local pelo Streamlit.
"""

GOLD_DIR = "/Volumes/govbr/gov_spending/raw_viagens/gold"
EXPORT_DIR = "/Volumes/govbr/gov_spending/raw_viagens/gold_export"

TABLES_TO_EXPORT = [
    "gasto_por_orgao_ano",
    "evolucao_anual",
    "ranking_orgaos",
    "custo_medio_por_viagem",
    "top_viajantes",
    "urgente_vs_normal",
    "sazonalidade_mensal",
    "meio_transporte",
    "duracao_media_por_orgao",
    "top_destinos",
    "gasto_per_capita_orgao",
    "outliers",
]

import os

os.makedirs(EXPORT_DIR, exist_ok=True)

for table in TABLES_TO_EXPORT:
    df = spark.read.format("delta").load(f"{GOLD_DIR}/{table}")  # noqa: F821
    pdf = df.toPandas()
    parquet_path = f"{EXPORT_DIR}/{table}.parquet"
    pdf.to_parquet(parquet_path, index=False)
    print(f"[export] {table}: {len(pdf):,} linhas -> {parquet_path}")
