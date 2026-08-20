"""
Transformações Bronze -> Silver.

Cada função:
1. Aplica tipagem correta (datas, valores monetários, booleanos)
2. Remove duplicatas exatas (comparando apenas as colunas de negócio,
   ignorando metadado de ingestão que varia entre execuções)
3. NÃO faz join entre tabelas — cada uma continua na sua granularidade
   original. O join fica pra camada Gold (ver docs/architecture.md).
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.utils.parsing import parse_br_date, parse_br_time, parse_br_decimal, parse_br_boolean

# colunas de metadado de ingestão — nunca entram na checagem de duplicata,
# porque mudam a cada execução (ingested_at) mesmo pra a mesma linha de dado
METADATA_COLUMNS = ["source_path", "source_year", "ingested_at"]


def _drop_exact_duplicates(df: DataFrame) -> DataFrame:
    """Remove duplicatas exatas nas colunas de negócio (ignora metadado)."""
    business_columns = [c for c in df.columns if c not in METADATA_COLUMNS]
    return df.dropDuplicates(business_columns)


def transform_viagem(df: DataFrame) -> DataFrame:
    df = df.withColumn("periodo_data_de_inicio", parse_br_date(F.col("periodo_data_de_inicio")))
    df = df.withColumn("periodo_data_de_fim", parse_br_date(F.col("periodo_data_de_fim")))
    df = df.withColumn("viagem_urgente", parse_br_boolean(F.col("viagem_urgente")))
    df = df.withColumn("valor_diarias", parse_br_decimal(F.col("valor_diarias")))
    df = df.withColumn("valor_passagens", parse_br_decimal(F.col("valor_passagens")))
    df = df.withColumn("valor_devolucao", parse_br_decimal(F.col("valor_devolucao")))
    df = df.withColumn("valor_outros_gastos", parse_br_decimal(F.col("valor_outros_gastos")))
    return _drop_exact_duplicates(df)


def transform_pagamento(df: DataFrame) -> DataFrame:
    df = df.withColumn("valor", parse_br_decimal(F.col("valor")))
    return _drop_exact_duplicates(df)


def transform_passagem(df: DataFrame) -> DataFrame:
    df = df.withColumn("valor_da_passagem", parse_br_decimal(F.col("valor_da_passagem")))
    df = df.withColumn("taxa_de_servico", parse_br_decimal(F.col("taxa_de_servico")))
    df = df.withColumn("data_da_emissao_compra", parse_br_date(F.col("data_da_emissao_compra")))
    df = df.withColumn("hora_da_emissao_compra", parse_br_time(F.col("hora_da_emissao_compra")))
    return _drop_exact_duplicates(df)


def transform_trecho(df: DataFrame) -> DataFrame:
    df = df.withColumn("origem_data", parse_br_date(F.col("origem_data")))
    df = df.withColumn("destino_data", parse_br_date(F.col("destino_data")))
    df = df.withColumn("numero_diarias", parse_br_decimal(F.col("numero_diarias")))
    return _drop_exact_duplicates(df)


# tabela -> função de transformação correspondente
TRANSFORMATIONS = {
    "viagem": transform_viagem,
    "pagamento": transform_pagamento,
    "passagem": transform_passagem,
    "trecho": transform_trecho,
}


def run_silver(spark, bronze_dir: str, silver_dir: str) -> None:
    """Lê cada tabela Bronze, aplica sua transformação, grava em Silver."""
    for table_name, transform_fn in TRANSFORMATIONS.items():
        bronze_path = f"{bronze_dir}/{table_name}"
        silver_path = f"{silver_dir}/{table_name}"

        df = spark.read.format("delta").load(bronze_path)
        before_count = df.count()

        df = transform_fn(df)
        after_count = df.count()

        df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(silver_path)

        dropped = before_count - after_count
        print(f"[silver] {table_name}: {after_count:,} registros ({dropped:,} duplicatas removidas) em {silver_path}")
