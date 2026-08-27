"""
Silver -> Gold transformations.

Strategy: build a central fact table ("fato_viagem" — one row per trip,
already with financial values, duration, and destination/transport mode)
and derive every business metric by aggregating on top of it. This
avoids repeating the trip/payment/ticket/leg join for every single
metric.

The join happens HERE, not in Silver (decision documented in
docs/architecture.md).
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


# ---------------------------------------------------------------------------
# Central fact table
# ---------------------------------------------------------------------------

def build_fato_viagem(spark: SparkSession, silver_dir: str) -> DataFrame:
    """
    Builds the fact table: one row per trip, combining trip data (values,
    agency, traveler) with aggregated leg data (main destination,
    transport mode, duration in daily allowances).
    """
    viagem = spark.read.format("delta").load(f"{silver_dir}/viagem")
    trecho = spark.read.format("delta").load(f"{silver_dir}/trecho")

    # aggregate legs per trip: take the first destination (lowest-sequence
    # leg) and its transport mode, sum the daily allowances
    trecho_principal = (
        trecho
        .withColumn(
            "rn",
            F.row_number().over(
                Window.partitionBy("identificador_do_processo_de_viagem")
                .orderBy(F.col("sequencia_trecho").asc())
            ),
        )
        .filter(F.col("rn") == 1)
        .select(
            "identificador_do_processo_de_viagem",
            F.col("destino_cidade").alias("destino_principal_cidade"),
            F.col("destino_uf").alias("destino_principal_uf"),
            F.col("meio_de_transporte").alias("meio_de_transporte_principal"),
        )
    )

    trecho_diarias = trecho.groupBy("identificador_do_processo_de_viagem").agg(
        F.sum("numero_diarias").alias("total_diarias_trecho")
    )

    fato = (
        viagem
        .withColumn(
            "valor_total_viagem",
            F.coalesce(F.col("valor_diarias"), F.lit(0.0))
            + F.coalesce(F.col("valor_passagens"), F.lit(0.0))
            + F.coalesce(F.col("valor_outros_gastos"), F.lit(0.0))
            - F.coalesce(F.col("valor_devolucao"), F.lit(0.0)),
        )
        .withColumn("ano", F.year("periodo_data_de_inicio"))
        .withColumn("mes", F.month("periodo_data_de_inicio"))
        .withColumn(
            "duracao_dias",
            F.datediff(F.col("periodo_data_de_fim"), F.col("periodo_data_de_inicio")),
        )
        .join(trecho_principal, on="identificador_do_processo_de_viagem", how="left")
        .join(trecho_diarias, on="identificador_do_processo_de_viagem", how="left")
    )

    return fato


# ---------------------------------------------------------------------------
# Metrics derived from the fact table
# ---------------------------------------------------------------------------

def gasto_por_orgao_ano(fato: DataFrame) -> DataFrame:
    return fato.groupBy("nome_do_orgao_superior", "ano").agg(
        F.sum("valor_total_viagem").alias("gasto_total"),
        F.count("*").alias("qtd_viagens"),
    )


def evolucao_anual(fato: DataFrame) -> DataFrame:
    return fato.groupBy("ano").agg(
        F.sum("valor_total_viagem").alias("gasto_total"),
        F.count("*").alias("qtd_viagens"),
        F.avg("valor_total_viagem").alias("gasto_medio_por_viagem"),
    ).orderBy("ano")


def ranking_orgaos(fato: DataFrame) -> DataFrame:
    return fato.groupBy("nome_do_orgao_superior").agg(
        F.sum("valor_total_viagem").alias("gasto_total"),
        F.count("*").alias("qtd_viagens"),
    ).orderBy(F.desc("gasto_total"))


def custo_medio_por_viagem(fato: DataFrame) -> DataFrame:
    """Overall average cost and per-agency average cost, side by side (a small table)."""
    geral = fato.agg(F.avg("valor_total_viagem").alias("custo_medio")).withColumn(
        "nome_do_orgao_superior", F.lit("TODOS OS ÓRGÃOS")
    )
    por_orgao = fato.groupBy("nome_do_orgao_superior").agg(
        F.avg("valor_total_viagem").alias("custo_medio")
    )
    return por_orgao.unionByName(geral).orderBy(F.desc("custo_medio"))


def top_viajantes(fato: DataFrame, top_n: int = 100) -> DataFrame:
    return (
        fato.groupBy("cpf_viajante", "nome")
        .agg(
            F.sum("valor_total_viagem").alias("gasto_total"),
            F.count("*").alias("qtd_viagens"),
        )
        .orderBy(F.desc("gasto_total"))
        .limit(top_n)
    )


def urgente_vs_normal(fato: DataFrame) -> DataFrame:
    return fato.groupBy("viagem_urgente").agg(
        F.avg("valor_total_viagem").alias("gasto_medio"),
        F.count("*").alias("qtd_viagens"),
    )


def sazonalidade_mensal(fato: DataFrame) -> DataFrame:
    return fato.groupBy("mes").agg(
        F.sum("valor_total_viagem").alias("gasto_total"),
        F.count("*").alias("qtd_viagens"),
    ).orderBy("mes")


def meio_transporte(fato: DataFrame) -> DataFrame:
    return fato.groupBy("meio_de_transporte_principal").agg(
        F.sum("valor_total_viagem").alias("gasto_total"),
        F.count("*").alias("qtd_viagens"),
        F.avg("valor_total_viagem").alias("gasto_medio"),
    ).orderBy(F.desc("gasto_total"))


def duracao_media_por_orgao(fato: DataFrame) -> DataFrame:
    return fato.groupBy("nome_do_orgao_superior").agg(
        F.avg("duracao_dias").alias("duracao_media_dias"),
        F.count("*").alias("qtd_viagens"),
    ).orderBy(F.desc("duracao_media_dias"))


def top_destinos(fato: DataFrame, top_n: int = 100) -> DataFrame:
    return (
        fato.groupBy("destino_principal_cidade", "destino_principal_uf")
        .agg(
            F.count("*").alias("qtd_viagens"),
            F.sum("valor_total_viagem").alias("gasto_total"),
        )
        .orderBy(F.desc("qtd_viagens"))
        .limit(top_n)
    )


def gasto_per_capita_orgao(fato: DataFrame) -> DataFrame:
    return fato.groupBy("nome_do_orgao_superior").agg(
        F.sum("valor_total_viagem").alias("gasto_total"),
        F.countDistinct("cpf_viajante").alias("qtd_viajantes_distintos"),
    ).withColumn(
        "gasto_per_capita",
        F.col("gasto_total") / F.col("qtd_viajantes_distintos"),
    ).orderBy(F.desc("gasto_per_capita"))


def outliers(fato: DataFrame, z_threshold: float = 3.0) -> DataFrame:
    """
    Flags trips whose valor_total_viagem is far above the mean (more
    than `z_threshold` standard deviations), computed per agency — this
    compares each trip against its own agency's norm, rather than the
    global average (which would be skewed by agencies that naturally take
    more expensive trips, e.g. international travel).
    """
    stats = fato.groupBy("nome_do_orgao_superior").agg(
        F.avg("valor_total_viagem").alias("_media_orgao"),
        F.stddev("valor_total_viagem").alias("_stddev_orgao"),
    )

    return (
        fato.join(stats, on="nome_do_orgao_superior", how="left")
        .withColumn(
            "z_score",
            (F.col("valor_total_viagem") - F.col("_media_orgao")) / F.col("_stddev_orgao"),
        )
        .withColumn("is_outlier", F.abs(F.col("z_score")) > z_threshold)
        .filter(F.col("is_outlier"))
        .select(
            "identificador_do_processo_de_viagem",
            "nome_do_orgao_superior",
            "nome",
            "ano",
            "valor_total_viagem",
            "z_score",
        )
        .orderBy(F.desc("z_score"))
    )


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------

GOLD_TABLES = {
    "fato_viagem": None,  # handled separately, it's the base table
    "gasto_por_orgao_ano": gasto_por_orgao_ano,
    "evolucao_anual": evolucao_anual,
    "ranking_orgaos": ranking_orgaos,
    "custo_medio_por_viagem": custo_medio_por_viagem,
    "top_viajantes": top_viajantes,
    "urgente_vs_normal": urgente_vs_normal,
    "sazonalidade_mensal": sazonalidade_mensal,
    "meio_transporte": meio_transporte,
    "duracao_media_por_orgao": duracao_media_por_orgao,
    "top_destinos": top_destinos,
    "gasto_per_capita_orgao": gasto_per_capita_orgao,
    "outliers": outliers,
}


def save_as_catalog_table(df: DataFrame, table_name: str, catalog: str, schema: str) -> int:
    """
    Writes a DataFrame as a MANAGED Unity Catalog table (no manual
    LOCATION — Databricks decides the storage automatically). Tables
    with a LOCATION inside a Volume aren't supported by UC (Volumes are
    for files, not table storage), so we use saveAsTable instead of
    save(path) + CREATE TABLE ... LOCATION.
    """
    full_name = f"{catalog}.{schema}.gold_{table_name}"
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(full_name)
    count = df.count()
    print(f"[gold] {table_name}: {count:,} records at {full_name}")
    return count


def run_gold(spark: SparkSession, silver_dir: str, catalog: str = "govbr", schema: str = "gov_spending") -> None:
    fato = build_fato_viagem(spark, silver_dir)
    save_as_catalog_table(fato, "fato_viagem", catalog, schema)

    for table_name, fn in GOLD_TABLES.items():
        if fn is None:
            continue
        df = fn(fato)
        save_as_catalog_table(df, table_name, catalog, schema)
