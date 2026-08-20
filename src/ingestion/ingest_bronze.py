"""
Ingestão Bronze — lê os CSVs brutos do Portal da Transparência (Viagem,
Pagamento, Passagem, Trecho), organizados em uma subpasta por ano
(ex: extraidos/2014/Viagem.csv, extraidos/2015/Viagem.csv, ...), e grava
em Delta Lake sem transformação, apenas com metadado de ingestão.

Os arquivos do Portal da Transparência seguem o padrão:
- separador: ";"
- encoding: "ISO-8859-1" (latin-1)
- decimal: "," (vírgula, padrão brasileiro)

Uso (dentro de um notebook Databricks, com `spark` já disponível):

    from src.ingestion.ingest_bronze import ingest_all

    ingest_all(
        spark,
        source_dir="/Volumes/govbr/gov_spending/raw_viagens/GOVBR/extraidos",
        bronze_dir="/Volumes/govbr/gov_spending/raw_viagens/bronze",
    )
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

# Nome do arquivo fonte -> nome da tabela Bronze correspondente
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
    "inferSchema": "false",  # Bronze: tudo como string, tipagem fica pra Silver
}


def read_raw_csv_all_years(spark: SparkSession, source_dir: str, filename: str) -> DataFrame:
    """
    Lê um tipo de arquivo (ex: Viagem.csv) de todas as subpastas de ano de
    uma vez, usando wildcard. Os arquivos reais seguem o padrão
    <ano>_<Filename>.csv dentro de cada subpasta de ano, ex:
    source_dir/2014/2014_Viagem.csv, source_dir/2015/2015_Viagem.csv, etc.
    """
    path_pattern = f"{source_dir}/*/*_{filename}"
    df = spark.read.options(**CSV_OPTIONS).csv(path_pattern)

    # extrai o ano a partir do nome do arquivo de origem
    # (input_file_name devolve algo como .../extraidos/2014/2014_Viagem.csv)
    df = df.withColumn("_source_path", F.input_file_name())
    df = df.withColumn(
        "_source_year",
        F.regexp_extract(F.col("_source_path"), r"/(\d{4})_[^/]+$", 1),
    )

    return df


def add_ingestion_metadata(df: DataFrame) -> DataFrame:
    """Adiciona coluna de metadado de ingestão (_source_path já vem de read_raw_csv_all_years)."""
    return df.withColumn("_ingested_at", F.current_timestamp())


def ingest_one(spark: SparkSession, source_dir: str, bronze_dir: str, filename: str, table_name: str) -> None:
    """Ingere um tipo de arquivo (todos os anos) para a camada Bronze em Delta Lake."""
    target_path = f"{bronze_dir}/{table_name}"

    df = read_raw_csv_all_years(spark, source_dir, filename)
    df = add_ingestion_metadata(df)

    df.write.format("delta").mode("overwrite").save(target_path)

    count = df.count()
    years = sorted(r["_source_year"] for r in df.select("_source_year").distinct().collect())
    print(f"[bronze] {table_name}: {count:,} registros gravados em {target_path} | anos: {years}")


def ingest_all(spark: SparkSession, source_dir: str, bronze_dir: str) -> None:
    """Ingere todos os arquivos conhecidos (Viagem, Pagamento, Passagem, Trecho), todos os anos."""
    for filename, table_name in SOURCE_FILES.items():
        ingest_one(spark, source_dir, bronze_dir, filename, table_name)
