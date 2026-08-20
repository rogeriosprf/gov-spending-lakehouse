"""
Transformações Silver -> Gold.

Estratégia: construir uma tabela fato central ("fato_viagem" — uma linha
por viagem, já com valores financeiros, duração e destino/meio de
transporte) e derivar todas as métricas de negócio por agregação em
cima dela. Evita repetir o join entre viagem/pagamento/passagem/trecho
para cada métrica.

O join acontece AQUI, não na Silver (decisão documentada em
docs/architecture.md).
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


# ---------------------------------------------------------------------------
# Tabela fato central
# ---------------------------------------------------------------------------

def build_fato_viagem(spark: SparkSession, silver_dir: str) -> DataFrame:
    """
    Constrói a tabela fato: uma linha por viagem, combinando dados de
    viagem (valores, órgão, viajante) com dados agregados de trecho
    (destino principal, meio de transporte, duração em diárias).
    """
    viagem = spark.read.format("delta").load(f"{silver_dir}/viagem")
    trecho = spark.read.format("delta").load(f"{silver_dir}/trecho")

    # agrega trecho por viagem: pega o primeiro destino (trecho de menor
    # sequência) e o meio de transporte mais frequente, soma as diárias
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
# Métricas derivadas da tabela fato
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
    """Custo médio geral e por órgão, lado a lado (uma tabela pequena)."""
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
    Marca viagens cujo valor_total_viagem está muito acima da média
    (mais de `z_threshold` desvios-padrão), calculado por órgão — assim
    compara-se cada viagem com o padrão do próprio órgão, não com a
    média geral (que seria distorcida por órgãos com viagens naturalmente
    mais caras, ex: internacionais).
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
# Orquestração
# ---------------------------------------------------------------------------

GOLD_TABLES = {
    "fato_viagem": None,  # tratado à parte, é a base
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


def run_gold(spark: SparkSession, silver_dir: str, gold_dir: str) -> None:
    fato = build_fato_viagem(spark, silver_dir)
    fato.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(
        f"{gold_dir}/fato_viagem"
    )
    fato_count = fato.count()
    print(f"[gold] fato_viagem: {fato_count:,} registros em {gold_dir}/fato_viagem")

    for table_name, fn in GOLD_TABLES.items():
        if fn is None:
            continue
        df = fn(fato)
        target_path = f"{gold_dir}/{table_name}"
        df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(target_path)
        count = df.count()
        print(f"[gold] {table_name}: {count:,} registros em {target_path}")
