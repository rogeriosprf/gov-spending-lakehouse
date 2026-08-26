"""
Notebook Databricks - Etapa 3: Agregação Gold

Constrói a tabela fato (join viagem + trecho) e todas as métricas de
negócio derivadas: gasto por órgão/ano, ranking de órgãos, evolução
anual, top viajantes, urgente vs normal, sazonalidade mensal, meio de
transporte, duração média, top destinos, gasto per capita e outliers.

Grava cada tabela como tabela GERENCIADA do Unity Catalog (não em Volume
path — Volumes não suportam LOCATION de tabela), prontas para consulta
via SQL Warehouse (ex: pelo dashboard Streamlit).
"""

from src.transformations.gold import run_gold

SILVER_DIR = "/Volumes/govbr/gov_spending/raw_viagens/silver"
CATALOG = "govbr"
SCHEMA = "gov_spending"

run_gold(spark, silver_dir=SILVER_DIR, catalog=CATALOG, schema=SCHEMA)  # noqa: F821
