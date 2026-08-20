"""
Utilitários de schema — sanitização de nomes de coluna para compatibilidade
com Delta Lake, que não aceita espaços, acentos ou caracteres especiais
(' ,;{}()\\n\\t=') nos nomes de coluna.

Os headers originais do Portal da Transparência vêm em português, com
espaço, acento e parênteses (ex: "Número da Proposta (PCDP)"). Aqui
convertemos para snake_case sem acento, preservando o significado.

Exemplo:
    "Número da Proposta (PCDP)" -> "numero_da_proposta_pcdp"
    "Período - Data de início"  -> "periodo_data_de_inicio"
"""

import re
import unicodedata

from pyspark.sql import DataFrame


def sanitize_column_name(name: str) -> str:
    """Converte um nome de coluna arbitrário para um formato seguro pro Delta Lake."""
    # remove acentos (ex: "número" -> "numero")
    nfkd = unicodedata.normalize("NFKD", name)
    ascii_name = nfkd.encode("ASCII", "ignore").decode("ASCII")

    # troca qualquer sequência de caracteres não alfanuméricos por underscore
    ascii_name = re.sub(r"[^0-9a-zA-Z]+", "_", ascii_name)

    # colapsa underscores repetidos e remove nas pontas
    ascii_name = re.sub(r"_+", "_", ascii_name).strip("_")

    return ascii_name.lower()


def sanitize_columns(df: DataFrame) -> DataFrame:
    """Renomeia todas as colunas de um DataFrame para nomes seguros pro Delta Lake."""
    for original in df.columns:
        df = df.withColumnRenamed(original, sanitize_column_name(original))
    return df
