"""
Bronze ingestion — reads the raw Portal da Transparência CSVs (trip,
payment, ticket, leg), organized in one subfolder per year (e.g.
extraidos/2014/2014_Viagem.csv, extraidos/2015/2015_Viagem.csv, ...), and
writes them to Delta Lake with no business transformation — only column
name sanitization (required by Delta Lake) and ingestion metadata.

Portal da Transparência files follow this pattern:
- separator: ";"
- encoding: "ISO-8859-1" (latin-1)
- decimal separator: "," (Brazilian standard)

Usage (inside a Databricks notebook, with `spark` already available):

    from src.ingestion.ingest_bronze import ingest_all

    ingest_all(
        spark,
        source_dir="/Volumes/govbr/gov_spending/raw_viagens/GOVBR/extraidos",
        bronze_dir="/Volumes/govbr/gov_spending/raw_viagens/bronze",
    )
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

from src.utils.schema import sanitize_columns

# source file name -> corresponding Bronze table name
SOURCE_FILES = {
    "Viagem.csv": "viagem",
    "Pagamento.csv": "pagamento",
    "Passagem.csv": "passagem",
    "Trecho.csv": "trecho",
}

CSV_OPTIONS = {
    "header": "true",
    "sep": ";",
    "encoding": "ISO-8859-1",
    "inferSchema": "false",  # Bronze: everything stays a string, typing happens in Silver
}


def read_raw_csv_all_years(spark: SparkSession, source_dir: str, filename: str) -> DataFrame:
    """
    Reads one file type (e.g. Viagem.csv) across all year subfolders at
    once, using a wildcard. Actual files follow the pattern
    <year>_<Filename>.csv inside each year subfolder, e.g.
    source_dir/2014/2014_Viagem.csv, source_dir/2015/2015_Viagem.csv, etc.

    Returns only the original CSV columns (no metadata yet) — column
    sanitization must run BEFORE any metadata column is added, or their
    "_" prefix gets stripped too (see sanitize_column_name).
    """
    path_pattern = f"{source_dir}/*/*_{filename}"
    return spark.read.options(**CSV_OPTIONS).csv(path_pattern)


def add_ingestion_metadata(df: DataFrame) -> DataFrame:
    """
    Adds ingestion metadata columns. Must be called AFTER
    sanitize_columns, so these columns aren't affected by sanitization.

    On Unity Catalog, input_file_name() isn't supported — the special
    _metadata.file_path column is used instead.
    """
    df = df.withColumn("source_path", F.col("_metadata.file_path"))
    df = df.withColumn(
        "source_year",
        F.regexp_extract(F.col("source_path"), r"/(\d{4})_[^/]+$", 1),
    )
    df = df.withColumn("ingested_at", F.current_timestamp())
    return df


def ingest_one(spark: SparkSession, source_dir: str, bronze_dir: str, filename: str, table_name: str) -> None:
    """Ingests one file type (all years) into the Bronze layer as Delta Lake."""
    target_path = f"{bronze_dir}/{table_name}"

    df = read_raw_csv_all_years(spark, source_dir, filename)
    df = sanitize_columns(df)          # sanitize the ORIGINAL columns first
    df = add_ingestion_metadata(df)    # only then add metadata (without a leading "_")

    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(target_path)

    count = df.count()
    years = sorted(r["source_year"] for r in df.select("source_year").distinct().collect())
    print(f"[bronze] {table_name}: {count:,} records written to {target_path} | years: {years}")


def ingest_all(spark: SparkSession, source_dir: str, bronze_dir: str) -> None:
    """Ingests all known files (trip, payment, ticket, leg), all years."""
    for filename, table_name in SOURCE_FILES.items():
        ingest_one(spark, source_dir, bronze_dir, filename, table_name)
