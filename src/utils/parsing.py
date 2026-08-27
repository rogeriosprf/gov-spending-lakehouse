"""
Parsing utilities for the Brazilian government data pattern:
- dates in dd/MM/yyyy format
- currency values in "1.234,56" format (dot = thousands, comma = decimal)
- booleans as "SIM"/"NAO" (Yes/No)
"""

from pyspark.sql import Column
from pyspark.sql import functions as F


def parse_br_date(col: Column) -> Column:
    """Converts a 'dd/MM/yyyy' string column to date. Invalid values become null."""
    return F.to_date(col, "dd/MM/yyyy")


def parse_br_time(col: Column) -> Column:
    """Converts a 'HH:mm' string column to a time timestamp. Invalid values become null."""
    return F.to_timestamp(col, "HH:mm")


def parse_br_decimal(col: Column) -> Column:
    """
    Converts a Brazilian-formatted string column ('1.234,56') to double.
    Strips the thousands separator (.) and swaps the decimal separator
    (,) for (.). Empty or invalid values become null.
    """
    cleaned = F.regexp_replace(col, r"\.", "")
    cleaned = F.regexp_replace(cleaned, r",", ".")
    return cleaned.cast("double")


def parse_br_boolean(col: Column) -> Column:
    """Converts 'SIM'/'NAO' (accented or not, any case) to boolean."""
    normalized = F.upper(F.trim(col))
    return F.when(normalized == "SIM", True).when(
        normalized.isin("NAO", "NÃO"), False
    ).otherwise(F.lit(None).cast("boolean"))
