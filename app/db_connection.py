"""
Conexão ao vivo com o Databricks SQL Warehouse.

Credenciais nunca ficam no código: vêm de st.secrets (Streamlit Cloud /
.streamlit/secrets.toml local) ou de variáveis de ambiente, nessa ordem
de prioridade.

Necessário (ver README.md para onde encontrar cada valor):
- DATABRICKS_SERVER_HOSTNAME  (Connection Details do SQL Warehouse)
- DATABRICKS_HTTP_PATH        (idem)
- DATABRICKS_TOKEN            (Personal Access Token, gerado no seu usuário)
"""

import os

import pandas as pd
import streamlit as st
from databricks import sql


def _get_secret(key: str) -> str:
    if key in st.secrets:
        return st.secrets[key]
    value = os.environ.get(key)
    if not value:
        raise RuntimeError(
            f"Credencial '{key}' não encontrada. Configure em "
            f".streamlit/secrets.toml ou como variável de ambiente."
        )
    return value


@st.cache_resource
def get_connection():
    return sql.connect(
        server_hostname=_get_secret("DATABRICKS_SERVER_HOSTNAME"),
        http_path=_get_secret("DATABRICKS_HTTP_PATH"),
        access_token=_get_secret("DATABRICKS_TOKEN"),
    )


@st.cache_data(ttl=600)  # cache de 10min — evita reconsultar o warehouse a cada interação do usuário
def query_table(table_name: str, catalog: str = "govbr", schema: str = "gov_spending") -> pd.DataFrame:
    """Consulta uma tabela Gold registrada no Unity Catalog, retorna como pandas DataFrame."""
    full_name = f"{catalog}.{schema}.gold_{table_name}"
    conn = get_connection()
    with conn.cursor() as cursor:
        cursor.execute(f"SELECT * FROM {full_name}")
        return cursor.fetchall_arrow().to_pandas()
