"""
Dashboard - Brazilian Federal Government Business Travel Spending (2011-2026)

Connects live to a Databricks SQL Warehouse and queries the Gold layer
tables directly from Unity Catalog. See README.md and
docs/architecture.md for full pipeline details.

Note: underlying column names come from the original Portuguese source
schema (Portal da Transparência) and are relabeled here only for display.
"""

import pandas as pd
import plotly.express as px
import streamlit as st

from db_connection import query_table

st.set_page_config(
    page_title="Government Travel Spending",
    page_icon="✈️",
    layout="wide",
)


def load_table(name: str) -> pd.DataFrame:
    return query_table(name)


st.title("✈️ Brazilian Federal Government Business Travel Spending")
st.caption(
    "Data from Portal da Transparência (2011-2026) — PySpark + Delta Lake + "
    "Databricks + Airflow pipeline. [View repository](https://github.com/rogeriosprf/gov-spending-lakehouse)"
)

tab_overview, tab_agencies, tab_profile, tab_rankings, tab_outliers = st.tabs(
    ["Overview", "By Agency", "Trip Profile", "Rankings", "Outliers"]
)

# ---------------------------------------------------------------------------
# Overview
# ---------------------------------------------------------------------------
with tab_overview:
    yearly = load_table("evolucao_anual")

    total_spend = yearly["gasto_total"].sum()
    total_trips = yearly["qtd_viagens"].sum()
    avg_cost = total_spend / total_trips if total_trips else 0

    col1, col2, col3 = st.columns(3)
    col1.metric("Total spend (2011-2026)", f"R$ {total_spend:,.0f}")
    col2.metric("Total trips", f"{total_trips:,.0f}")
    col3.metric("Average cost per trip", f"R$ {avg_cost:,.2f}")

    fig = px.line(
        yearly,
        x="ano",
        y="gasto_total",
        markers=True,
        title="Total spend by year",
        labels={"ano": "Year", "gasto_total": "Total spend (R$)"},
    )
    st.plotly_chart(fig, use_container_width=True)

    fig_qty = px.bar(
        yearly,
        x="ano",
        y="qtd_viagens",
        title="Number of trips by year",
        labels={"ano": "Year", "qtd_viagens": "Trips"},
    )
    st.plotly_chart(fig_qty, use_container_width=True)

# ---------------------------------------------------------------------------
# By Agency
# ---------------------------------------------------------------------------
with tab_agencies:
    ranking = load_table("ranking_orgaos").sort_values("gasto_total", ascending=False).head(20)
    per_capita = load_table("gasto_per_capita_orgao").sort_values("gasto_per_capita", ascending=False).head(20)
    duration = load_table("duracao_media_por_orgao").sort_values("duracao_media_dias", ascending=False).head(20)

    fig_ranking = px.bar(
        ranking,
        x="gasto_total",
        y="nome_do_orgao_superior",
        orientation="h",
        title="Top 20 agencies by total spend",
        labels={"gasto_total": "Total spend (R$)", "nome_do_orgao_superior": ""},
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
            title="Top 20 by spend per capita",
            labels={"gasto_per_capita": "Spend per capita (R$)", "nome_do_orgao_superior": ""},
        )
        fig_pc.update_layout(yaxis={"categoryorder": "total ascending"})
        st.plotly_chart(fig_pc, use_container_width=True)
    with col2:
        fig_dur = px.bar(
            duration,
            x="duracao_media_dias",
            y="nome_do_orgao_superior",
            orientation="h",
            title="Top 20 by average trip duration",
            labels={"duracao_media_dias": "Average duration (days)", "nome_do_orgao_superior": ""},
        )
        fig_dur.update_layout(yaxis={"categoryorder": "total ascending"})
        st.plotly_chart(fig_dur, use_container_width=True)

# ---------------------------------------------------------------------------
# Trip Profile
# ---------------------------------------------------------------------------
with tab_profile:
    urgent = load_table("urgente_vs_normal")
    seasonality = load_table("sazonalidade_mensal")
    transport = load_table("meio_transporte")

    col1, col2 = st.columns(2)
    with col1:
        urgent_label = urgent.copy()
        urgent_label["viagem_urgente"] = urgent_label["viagem_urgente"].map(
            {True: "Urgent", False: "Regular"}
        )
        fig_urg = px.bar(
            urgent_label,
            x="viagem_urgente",
            y="gasto_medio",
            title="Average spend: urgent vs. regular trips",
            labels={"viagem_urgente": "", "gasto_medio": "Average spend (R$)"},
        )
        st.plotly_chart(fig_urg, use_container_width=True)

    with col2:
        fig_transp = px.pie(
            transport,
            names="meio_de_transporte_principal",
            values="qtd_viagens",
            title="Distribution by transport mode",
        )
        st.plotly_chart(fig_transp, use_container_width=True)

    months = [
        "Jan", "Feb", "Mar", "Apr", "May", "Jun",
        "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
    ]
    seasonality = seasonality.sort_values("mes")
    seasonality["mes_nome"] = seasonality["mes"].apply(lambda m: months[int(m) - 1])
    fig_saz = px.bar(
        seasonality,
        x="mes_nome",
        y="qtd_viagens",
        title="Seasonality — number of trips by month",
        labels={"mes_nome": "Month", "qtd_viagens": "Trips"},
    )
    st.plotly_chart(fig_saz, use_container_width=True)

# ---------------------------------------------------------------------------
# Rankings
# ---------------------------------------------------------------------------
with tab_rankings:
    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Top travelers by accumulated spend")
        top_travelers = load_table("top_viajantes")[["nome", "gasto_total", "qtd_viagens"]].rename(
            columns={"nome": "Name", "gasto_total": "Total spend", "qtd_viagens": "Trips"}
        )
        st.dataframe(top_travelers, use_container_width=True, hide_index=True)
    with col2:
        st.subheader("Top destinations by trip volume")
        top_destinations = load_table("top_destinos").rename(
            columns={
                "destino_principal_cidade": "City",
                "destino_principal_uf": "State",
                "qtd_viagens": "Trips",
                "gasto_total": "Total spend",
            }
        )
        st.dataframe(top_destinations, use_container_width=True, hide_index=True)

# ---------------------------------------------------------------------------
# Outliers
# ---------------------------------------------------------------------------
with tab_outliers:
    st.subheader("Trips priced well above the agency's own norm")
    st.caption(
        "Computed via z-score within each agency (> 3 standard deviations from that agency's "
        "mean), not the global average — this avoids flagging agencies that naturally travel "
        "more expensively (e.g. international trips) as outliers. See docs/architecture.md for "
        "a known limitation of this method (spend distribution is long-tailed; a percentile-based "
        "approach would be more robust than z-score)."
    )
    outliers_df = load_table("outliers").sort_values("z_score", ascending=False).rename(
        columns={
            "identificador_do_processo_de_viagem": "Trip ID",
            "nome_do_orgao_superior": "Agency",
            "nome": "Traveler",
            "ano": "Year",
            "valor_total_viagem": "Total value",
            "z_score": "Z-score",
        }
    )
    st.dataframe(outliers_df.head(200), use_container_width=True, hide_index=True)
