"""
Notebook Databricks - Etapa 3: Agregação Gold

Constrói a tabela fato (join viagem + trecho) e todas as métricas de
negócio derivadas: gasto por órgão/ano, ranking de órgãos, evolução
anual, top viajantes, urgente vs normal, sazonalidade mensal, meio de
transporte, duração média, top destinos, gasto per capita e outliers.
"""

from src.transformations.gold import run_gold

SILVER_DIR = "/Volumes/govbr/gov_spending/raw_viagens/silver"
GOLD_DIR = "/Volumes/govbr/gov_spending/raw_viagens/gold"

run_gold(spark, silver_dir=SILVER_DIR, gold_dir=GOLD_DIR)  # noqa: F821
