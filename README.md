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

## Status

🚧 Em construção. Ver [docs/architecture.md](docs/architecture.md) para
decisões de escopo e o que ainda não foi implementado.

## Autor

Paulo Rogério — [portfólio](https://rogeriosprf.github.io/portifolio/)
