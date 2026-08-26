# Gov Spending Lakehouse

Pipeline de dados em arquitetura Medallion (Bronze/Silver/Gold) sobre dados
públicos de despesas de viagens do governo federal brasileiro (Portal da
Transparência), ~9M de registros.

Projeto de portfólio construído para demonstrar experiência prática com o
stack moderno de engenharia de dados em nuvem: **PySpark, Delta Lake,
Databricks e Airflow**.

## Stack

- **Processamento**: PySpark
- **Armazenamento**: Delta Lake (formato ACID sobre Parquet)
- **Ambiente**: Databricks Community Edition (gratuito)
- **Orquestração**: Apache Airflow
- **Fonte de dados**: Portal da Transparência (governo federal brasileiro)

## Arquitetura

```
Fonte (Portal da Transparência)
        │
        ▼
   ┌─────────┐      ┌─────────┐      ┌─────────┐
   │ BRONZE  │ ───► │ SILVER  │ ───► │  GOLD   │
   │  (raw)  │      │(limpo)  │      │(agregado)│
   └─────────┘      └─────────┘      └─────────┘
        orquestrado por Airflow (dags/)
```

Detalhes de cada camada em [docs/architecture.md](docs/architecture.md).

## Estrutura do repositório

```
gov-spending-lakehouse/
├── data/                   # não versionado (ver .gitignore) — camadas locais de teste
│   ├── bronze/
│   ├── silver/
│   └── gold/
├── notebooks/              # notebooks Databricks (uma etapa por arquivo)
├── dags/                   # DAGs do Airflow
├── src/
│   ├── ingestion/          # scripts de coleta da fonte
│   ├── transformations/    # lógica de transformação Bronze→Silver→Gold
│   └── utils/              # helpers compartilhados
├── docs/
│   └── architecture.md
└── tests/
```

## Dashboard

Um dashboard Streamlit se conecta **ao vivo** ao Databricks SQL Warehouse
(via `databricks-sql-connector`) e consulta as tabelas Gold diretamente do
Unity Catalog — sem exportação manual de arquivo.

Setup:

1. Copie `.streamlit/secrets.toml.example` para `.streamlit/secrets.toml`
   e preencha com as credenciais do seu SQL Warehouse (instruções dentro
   do próprio arquivo de exemplo). Esse arquivo nunca é versionado.
2. Instale as dependências e rode:

```bash
pip install -r requirements-app.txt
streamlit run app/streamlit_app.py
```

As tabelas Gold precisam estar registradas no Unity Catalog (feito
automaticamente pelo pipeline, em `run_gold()`, como
`govbr.gov_spending.gold_<nome_da_tabela>`).

## Status

✅ Pipeline completo: Bronze → Silver → Gold (13 tabelas) → Dashboard.

16 anos de dados (2011-2026), ~9.7M de viagens processadas.

Ver [docs/architecture.md](docs/architecture.md) para decisões de escopo
e o que ainda não foi implementado (Terraform, AWS nativo, orquestração
via Airflow em produção).

## Autor

Paulo Rogério — [portfólio](https://rogeriosprf.github.io/portifolio/)
