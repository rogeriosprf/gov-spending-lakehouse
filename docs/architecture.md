# Architecture

## Layers (Medallion Architecture)

### Bronze — raw data
- Direct ingestion from the source (Portal da Transparência), no transformation.
- Written to Delta Lake, with an `ingested_at` metadata column and a
  `source_year` column derived from the file name.
- Goal: keep an auditable history of the data exactly as received.

### Silver — cleaned data
- Correct typing of columns (dates, currency values, booleans).
- Deduplication and null handling.
- Column names sanitized (Delta Lake doesn't allow spaces, accents or
  special characters in column names — original Portuguese headers are
  converted to snake_case, e.g. `"Número da Proposta (PCDP)"` →
  `numero_da_proposta_pcdp`).
- No join between tables at this stage — each table (trip, payment,
  ticket, leg) stays at its original granularity. The join happens in
  Gold (see decision below).
- **Real data quality finding**: deduplication removed 648,055 duplicate
  records from the payment table (~4% of the table) — worth mentioning
  in an interview as a concrete example of catching a real issue, not
  just running a pipeline end to end.

### Gold — aggregated data
- Business-ready metrics: total spend by agency, by period, by expense
  type, top travelers, seasonality, transport mode, outliers, etc.
- Built from a single central fact table (`fato_viagem`) that joins
  trip + leg data once — every other metric is derived from it by
  aggregation, instead of repeating the join per metric.
- Stored as **managed Unity Catalog tables** (`saveAsTable`), not as a
  Delta path inside a Volume — Volumes don't support table `LOCATION`
  (`Missing cloud file system scheme` error), so Gold uses a different
  storage mechanism than Bronze/Silver, which remain Delta paths inside
  the Volume.

## Orchestration

A single Airflow DAG (`dags/`) chains: ingestion → Bronze → Silver →
Gold, with retries configured per task. A failure in one stage blocks
the next one.

## Dashboard

A Streamlit dashboard connects **live** to a Databricks SQL Warehouse
via `databricks-sql-connector`, querying the Gold tables directly from
Unity Catalog. No static export step — this mirrors how real BI tools
(Power BI, Tableau) connect to a production Lakehouse.

## Scope decisions

- **Databricks Free Edition**: free, but with cluster limits
  (memory/session time). With 9.7M records, partitioning at ingestion
  time (by date, by agency) is required to fit the free tier — a
  decision also relevant for production scale.
- **AWS (Glue, Lakeformation, native Unity Catalog on AWS)**: out of
  scope for this first version. Databricks Free Edition already
  includes its own catalog (Unity Catalog basics), which partially
  covers the cataloging need without depending on AWS.
- **Terraform**: out of scope for this first version — the project
  currently runs manually/locally. Documented here as a real next step,
  not yet implemented.
- **Outlier detection method**: outliers are flagged using a z-score
  (> 3 standard deviations from the mean) computed **per agency**, not
  against the global average — this avoids flagging agencies that
  naturally travel more expensively (e.g. international trips) as
  outliers. Known limitation: trip spending is a long-tail distribution
  (mostly cheap, a few very expensive), so z-score against std-dev is
  not ideal here — a percentile-based method (e.g. p99) would be more
  robust. Left as a documented next step rather than implemented, to
  avoid overclaiming statistical rigor that wasn't actually applied.

## Next steps (not implemented)

- [ ] IaC with Terraform to provision the environment
- [ ] Managed Airflow deployment (instead of running locally)
- [ ] Automated data quality tests (e.g. Great Expectations)
- [ ] Percentile-based outlier detection instead of z-score
