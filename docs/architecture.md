# Arquitetura

## Camadas (Medallion Architecture)

### Bronze — dados brutos
- Ingestão direta da fonte (Portal da Transparência), sem transformação.
- Gravado em Delta Lake, com coluna de metadado `_ingested_at`.
- Objetivo: manter histórico auditável do dado exatamente como recebido.

### Silver — dados limpos
- Tipagem correta de colunas (datas, valores monetários, categóricas).
- Deduplicação e tratamento de nulos.
- Padronização de nomes de órgãos/categorias.
- Regras de validação de qualidade (ex: valores negativos, datas fora de
  intervalo esperado) documentadas em `src/transformations/`.

### Gold — dados agregados
- Métricas de negócio prontas para consumo: gasto total por órgão, por
  período, por tipo de despesa.
- Formato otimizado para leitura por dashboard/BI.
- Gravada como **tabelas gerenciadas do Unity Catalog** (`saveAsTable`),
  não como Delta path dentro de um Volume — Volumes não suportam
  `LOCATION` de tabela (erro `Missing cloud file system scheme`), então
  a Gold usa um mecanismo de storage diferente da Bronze/Silver, que
  continuam como Delta path dentro do Volume.

## Orquestração

Uma DAG única no Airflow (`dags/`) encadeia: ingestão → Bronze → Silver →
Gold, com retry configurado em cada tarefa. Falha em uma etapa não avança
para a próxima.

## Decisões de escopo

- **Databricks Community Edition**: gratuito, mas com limite de cluster
  (memória/tempo de sessão). Com 9M de registros, o particionamento na
  ingestão (por data, por órgão) é necessário para caber no free tier —
  essa é uma decisão pensada também para produção em escala maior.
- **AWS (Glue, Lakeformation, Unity Catalog nativo)**: fora do escopo desta
  primeira versão. O Databricks Community Edition já inclui um catálogo
  próprio (Hive metastore / Unity Catalog básico), que cobre parcialmente
  a necessidade de cataloging sem depender de AWS.
- **Terraform**: fora do escopo desta primeira versão — o projeto roda hoje
  de forma manual/local. Fica documentado aqui como próximo passo real, não
  implementado ainda.

## Próximos passos (não implementados)

- [ ] IaC com Terraform para provisionar o ambiente
- [ ] Deploy de Airflow gerenciado (ex: em vez de rodar local)
- [ ] Testes de qualidade de dados automatizados (ex: Great Expectations)
