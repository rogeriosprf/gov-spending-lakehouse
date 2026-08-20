"""
Utilitários de parsing para o padrão de dados do governo brasileiro:
- datas no formato dd/MM/yyyy
- valores monetários no formato "1.234,56" (ponto = milhar, vírgula = decimal)
- booleanos como "SIM"/"NÃO"
"""

from pyspark.sql import Column
from pyspark.sql import functions as F


def parse_br_date(col: Column) -> Column:
    """Converte uma coluna string 'dd/MM/yyyy' para date. Valores inválidos viram null."""
    return F.to_date(col, "dd/MM/yyyy")


def parse_br_time(col: Column) -> Column:
    """Converte uma coluna string 'HH:mm' para timestamp de hora. Valores inválidos viram null."""
    return F.to_timestamp(col, "HH:mm")


def parse_br_decimal(col: Column) -> Column:
    """
    Converte uma coluna string no formato brasileiro ('1.234,56') para
    double. Remove separador de milhar (.) e troca separador decimal
    (,) por (.). Valores vazios ou inválidos viram null.
    """
    cleaned = F.regexp_replace(col, r"\.", "")
    cleaned = F.regexp_replace(cleaned, r",", ".")
    return cleaned.cast("double")


def parse_br_boolean(col: Column) -> Column:
    """Converte 'SIM'/'NAO' (com ou sem acento, qualquer caixa) para boolean."""
    normalized = F.upper(F.trim(col))
    return F.when(normalized == "SIM", True).when(
        normalized.isin("NAO", "NÃO"), False
    ).otherwise(F.lit(None).cast("boolean"))
