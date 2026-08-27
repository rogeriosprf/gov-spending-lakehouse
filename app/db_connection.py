"""
Live connection to the Databricks SQL Warehouse.

Credentials are never hardcoded: they come from st.secrets (Streamlit
Cloud / local .streamlit/secrets.toml) or environment variables, in that
priority order.

Required (see README.md for where to find each value):
- DATABRICKS_SERVER_HOSTNAME  (SQL Warehouse "Connection details")
- DATABRICKS_HTTP_PATH        (same tab)
- DATABRICKS_TOKEN            (Personal Access Token, generated for your user)
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
            f"Credential '{key}' not found. Configure it in "
            f".streamlit/secrets.toml or as an environment variable."
        )
    return value


@st.cache_resource
def get_connection():
    return sql.connect(
        server_hostname=_get_secret("DATABRICKS_SERVER_HOSTNAME"),
        http_path=_get_secret("DATABRICKS_HTTP_PATH"),
        access_token=_get_secret("DATABRICKS_TOKEN"),
    )


@st.cache_data(ttl=600)  # 10min cache — avoids re-querying the warehouse on every user interaction
def query_table(table_name: str, catalog: str = "govbr", schema: str = "gov_spending") -> pd.DataFrame:
    """Queries a Gold table registered in Unity Catalog, returns it as a pandas DataFrame."""
    full_name = f"{catalog}.{schema}.gold_{table_name}"
    conn = get_connection()
    with conn.cursor() as cursor:
        cursor.execute(f"SELECT * FROM {full_name}")
        return cursor.fetchall_arrow().to_pandas()
