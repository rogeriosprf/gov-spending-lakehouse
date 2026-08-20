"""
Notebook Databricks - Etapa 2: Transformação Silver

Lê cada tabela Bronze, aplica tipagem (datas, valores monetários,
booleanos) e remove duplicatas exatas. Sem join entre tabelas — cada
uma continua na granularidade original. Grava em Delta Lake.
"""

from src.transformations.silver import run_silver

BRONZE_DIR = "/Volumes/govbr/gov_spending/raw_viagens/bronze"
SILVER_DIR = "/Volumes/govbr/gov_spending/raw_viagens/silver"

run_silver(spark, bronze_dir=BRONZE_DIR, silver_dir=SILVER_DIR)  # noqa: F821
