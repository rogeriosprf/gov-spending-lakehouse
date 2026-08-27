"""
Schema utilities — column name sanitization for Delta Lake compatibility,
which doesn't allow spaces, accents, or special characters
(' ,;{}()\\n\\t=') in column names.

Original Portal da Transparência headers come in Portuguese, with
spaces, accents, and parentheses (e.g. "Número da Proposta (PCDP)").
Here we convert them to snake_case without accents, preserving meaning.

Example:
    "Número da Proposta (PCDP)" -> "numero_da_proposta_pcdp"
    "Período - Data de início"  -> "periodo_data_de_inicio"
"""

import re
import unicodedata

from pyspark.sql import DataFrame


def sanitize_column_name(name: str) -> str:
    """Converts an arbitrary column name into a Delta Lake-safe format."""
    # strip accents (e.g. "número" -> "numero")
    nfkd = unicodedata.normalize("NFKD", name)
    ascii_name = nfkd.encode("ASCII", "ignore").decode("ASCII")

    # replace any run of non-alphanumeric characters with underscore
    ascii_name = re.sub(r"[^0-9a-zA-Z]+", "_", ascii_name)

    # collapse repeated underscores and trim from both ends
    ascii_name = re.sub(r"_+", "_", ascii_name).strip("_")

    return ascii_name.lower()


def sanitize_columns(df: DataFrame) -> DataFrame:
    """Renames every column of a DataFrame to a Delta Lake-safe name."""
    for original in df.columns:
        df = df.withColumnRenamed(original, sanitize_column_name(original))
    return df
