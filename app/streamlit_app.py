"""
Dashboard — Gastos com Viagens do Governo Federal Brasileiro (2011-2026)

Consome as tabelas Gold exportadas pelo pipeline (data/gold_export/*.csv),
geradas a partir do pipeline Bronze -> Silver -> Gold em PySpark/Delta
Lake/Databricks. Ver README.md e docs/architecture.md para detalhes do
pipeline completo.
"""

import pandas as pd
import plotly.express as px
import streamlit as st

from db_connection import query_table

st.set_page_config(
    page_title="Gastos com Viagens - Governo Federal",
    page_icon="✈️",
    layout="wide",
)


def load_table(name: str) -> pd.DataFrame:
    return query_table(name)


st.title("✈️ Gastos com Viagens do Governo Federal Brasileiro")
st.caption(
    "Dados do Portal da Transparência (2011-2026) — pipeline PySpark + Delta Lake + "
    "Databricks + Airflow. [Ver repositório](https://github.com/rogeriosprf/gov-spending-lakehouse)"
)

tab_overview, tab_orgaos, tab_perfil, tab_rankings, tab_outliers = st.tabs(
    ["Visão Geral", "Por Órgão", "Perfil da Viagem", "Rankings", "Outliers"]
)

# ---------------------------------------------------------------------------
# Visão Geral
# ---------------------------------------------------------------------------
with tab_overview:
    evolucao = load_table("evolucao_anual")

    total_gasto = evolucao["gasto_total"].sum()
    total_viagens = evolucao["qtd_viagens"].sum()
    custo_medio_geral = total_gasto / total_viagens if total_viagens else 0

    col1, col2, col3 = st.columns(3)
    col1.metric("Gasto total (2011-2026)", f"R$ {total_gasto:,.0f}")
    col2.metric("Total de viagens", f"{total_viagens:,.0f}")
    col3.metric("Custo médio por viagem", f"R$ {custo_medio_geral:,.2f}")

    fig = px.line(
        evolucao,
        x="ano",
        y="gasto_total",
        markers=True,
        title="Evolução do gasto total por ano",
        labels={"ano": "Ano", "gasto_total": "Gasto total (R$)"},
    )
    st.plotly_chart(fig, use_container_width=True)

    fig_qtd = px.bar(
        evolucao,
        x="ano",
        y="qtd_viagens",
        title="Quantidade de viagens por ano",
        labels={"ano": "Ano", "qtd_viagens": "Qtd. viagens"},
    )
    st.plotly_chart(fig_qtd, use_container_width=True)

# ---------------------------------------------------------------------------
# Por Órgão
# ---------------------------------------------------------------------------
with tab_orgaos:
    ranking = load_table("ranking_orgaos").sort_values("gasto_total", ascending=False).head(20)
    per_capita = load_table("gasto_per_capita_orgao").sort_values("gasto_per_capita", ascending=False).head(20)
    duracao = load_table("duracao_media_por_orgao").sort_values("duracao_media_dias", ascending=False).head(20)

    fig_ranking = px.bar(
        ranking,
        x="gasto_total",
        y="nome_do_orgao_superior",
        orientation="h",
        title="Top 20 órgãos por gasto total",
        labels={"gasto_total": "Gasto total (R$)", "nome_do_orgao_superior": ""},
    )
    fig_ranking.update_layout(yaxis={"categoryorder": "total ascending"})
    st.plotly_chart(fig_ranking, use_container_width=True)

    col1, col2 = st.columns(2)
    with col1:
        fig_pc = px.bar(
            per_capita,
            x="gasto_per_capita",
            y="nome_do_orgao_superior",
            orientation="h",
            title="Top 20 por gasto per capita",
            labels={"gasto_per_capita": "Gasto per capita (R$)", "nome_do_orgao_superior": ""},
        )
        fig_pc.update_layout(yaxis={"categoryorder": "total ascending"})
        st.plotly_chart(fig_pc, use_container_width=True)
    with col2:
        fig_dur = px.bar(
            duracao,
            x="duracao_media_dias",
            y="nome_do_orgao_superior",
            orientation="h",
            title="Top 20 por duração média de viagem",
            labels={"duracao_media_dias": "Duração média (dias)", "nome_do_orgao_superior": ""},
        )
        fig_dur.update_layout(yaxis={"categoryorder": "total ascending"})
        st.plotly_chart(fig_dur, use_container_width=True)

# ---------------------------------------------------------------------------
# Perfil da Viagem
# ---------------------------------------------------------------------------
with tab_perfil:
    urgente = load_table("urgente_vs_normal")
    sazonalidade = load_table("sazonalidade_mensal")
    transporte = load_table("meio_transporte")

    col1, col2 = st.columns(2)
    with col1:
        urgente_label = urgente.copy()
        urgente_label["viagem_urgente"] = urgente_label["viagem_urgente"].map(
            {True: "Urgente", False: "Normal"}
        )
        fig_urg = px.bar(
            urgente_label,
            x="viagem_urgente",
            y="gasto_medio",
            title="Gasto médio: viagem urgente vs normal",
            labels={"viagem_urgente": "", "gasto_medio": "Gasto médio (R$)"},
        )
        st.plotly_chart(fig_urg, use_container_width=True)

    with col2:
        fig_transp = px.pie(
            transporte,
            names="meio_de_transporte_principal",
            values="qtd_viagens",
            title="Distribuição por meio de transporte",
        )
        st.plotly_chart(fig_transp, use_container_width=True)

    meses = [
        "Jan", "Fev", "Mar", "Abr", "Mai", "Jun",
        "Jul", "Ago", "Set", "Out", "Nov", "Dez",
    ]
    sazonalidade = sazonalidade.sort_values("mes")
    sazonalidade["mes_nome"] = sazonalidade["mes"].apply(lambda m: meses[int(m) - 1])
    fig_saz = px.bar(
        sazonalidade,
        x="mes_nome",
        y="qtd_viagens",
        title="Sazonalidade — quantidade de viagens por mês",
        labels={"mes_nome": "Mês", "qtd_viagens": "Qtd. viagens"},
    )
    st.plotly_chart(fig_saz, use_container_width=True)

# ---------------------------------------------------------------------------
# Rankings
# ---------------------------------------------------------------------------
with tab_rankings:
    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Top viajantes por gasto acumulado")
        st.dataframe(
            load_table("top_viajantes")[["nome", "gasto_total", "qtd_viagens"]],
            use_container_width=True,
            hide_index=True,
        )
    with col2:
        st.subheader("Top destinos por volume de viagens")
        st.dataframe(
            load_table("top_destinos"),
            use_container_width=True,
            hide_index=True,
        )

# ---------------------------------------------------------------------------
# Outliers
# ---------------------------------------------------------------------------
with tab_outliers:
    st.subheader("Viagens com valor muito acima do padrão do próprio órgão")
    st.caption(
        "Calculado por z-score dentro de cada órgão (> 3 desvios-padrão da média do órgão), "
        "não pela média geral — evita marcar como outlier órgãos que naturalmente viajam mais caro "
        "(ex: viagens internacionais). Ver docs/architecture.md para a limitação conhecida desse método "
        "(distribuição de gasto é assimétrica; percentil seria mais robusto que z-score)."
    )
    outliers_df = load_table("outliers").sort_values("z_score", ascending=False)
    st.dataframe(outliers_df.head(200), use_container_width=True, hide_index=True)
