# Raw Data Storage (Task 3)

We store ingested data in a local data lake under `data/raw/`.

## Partitioning Strategy
Data is partitioned by:
- dataset type: interactions / products
- source: csv / api
- ingestion timestamp: date + hour (UTC)

## Folder Layout
data/raw/<dataset>/source=<source>/date=YYYY-MM-DD/hour=HH/<files>

## Examples
- Interactions (CSV):
  data/raw/interactions/source=csv/date=2026-01-16/hour=09/interactions_sample.parquet

- Products (API):
  data/raw/products/source=api/date=2026-01-16/hour=09/products.json

## Why this structure?
- Supports incremental processing (process only new partitions)
- Enables backfills (re-run older partitions)
- Clear lineage: raw partitions map to ingestion runs
