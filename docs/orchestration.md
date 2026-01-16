# Task 10: Pipeline Orchestration (Prefect)

## Tool
Prefect is used to orchestrate the end-to-end pipeline on Windows.

## Flow file
src/orchestration/prefect_flow.py

## How to run
1. Activate venv
2. Run:
   py -m src.orchestration.prefect_flow

## What it runs
- Ingestion: CSV + API
- Validation: schema + missing values + duplicates + range checks
- Reporting: Data Quality PDF
- Preparation + EDA
- Feature engineering + warehouse materialization
- Model training + evaluation + MLflow logging

## Evidence
- Prefect console logs / UI screenshots
- logs/pipeline.log
