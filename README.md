# RecoMart – End-to-End Data Management & ML Pipeline

This repository implements an end-to-end data management and machine learning pipeline for a recommendation system built for RecoMart, an e-commerce startup.

The project demonstrates modern practices in data engineering and MLOps, including data ingestion, validation, preparation, feature engineering, feature store design, data versioning, model training, experiment tracking, and orchestration.

---

## Project Objectives

- Build a scalable and maintainable recommendation pipeline
- Ensure data quality and reproducibility
- Enable reusable, versioned features for training and inference
- Track experiments and model performance
- Automate the full pipeline using orchestration

---

## High-Level Architecture

```text
CSV & API Ingestion
        ↓
Raw Data Storage (Partitioned)
        ↓
Data Validation & Profiling
        ↓
Data Preparation & EDA
        ↓
Feature Engineering + Warehouse (SQLite)
        ↓
Feature Store
        ↓
Model Training & Evaluation (MLflow)
        ↓
Pipeline Orchestration (Prefect)


## Repository Structure

```text
recomart-pipeline/
├── src/
│   ├── ingestion/
│   ├── validation/
│   ├── preparation/
│   ├── transformation/
│   ├── feature_store/
│   ├── modeling/
│   ├── orchestration/
│   ├── common/
│   └── config.py
│
├── docs/
│   ├── storage_structure.md
│   ├── feature_logic.md
│   ├── feature_store.md
│   ├── dvc_workflow.md
│   ├── lineage.md
│   └── orchestration.md
│
├── data/
│   ├── raw.dvc
│   ├── validated.dvc
│   ├── prepared.dvc
│   ├── features.dvc
│   └── warehouse.dvc
│
├── requirements.txt
├── README.md
├── .gitignore
├── .dvc/
└── .dvcignore
```

**Note:** Actual data files are not committed to Git.  
All datasets are versioned and managed using **DVC**.

---

## Prerequisites

- Python 3.9 or higher
- Git
- DVC
- Windows OS (tested environment)

---

## Setup Instructions

1. Clone the repository
   git clone <repository-url>
   cd recomart-pipeline

2. Create and activate virtual environment (Windows)
   py -m venv .venv
   .\.venv\Scripts\activate

3. Install dependencies
   pip install -r requirements.txt

---

## Data Versioning (DVC)

If you have access to the DVC remote:
   dvc pull

Otherwise, the pipeline can regenerate all datasets locally.

---

## Run the End-to-End Pipeline

   python -m src.orchestration.prefect_flow

This runs:
- CSV & API ingestion
- Data validation and quality reporting
- Data preparation and EDA
- Feature engineering and warehouse creation
- Feature store materialization
- Model training and evaluation
- MLflow experiment logging

---

## Experiment Tracking (MLflow)

Start the MLflow UI:
   mlflow ui

Open the displayed URL (usually http://127.0.0.1:5000).

---

## Feature Store Demo

   python -m src.feature_store.demo_retrieve_features

---

## Model Summary

- Model type: Popularity + Co-occurrence recommender
- Features: User and item aggregates (7-day windows)
- Metrics: Precision@K, Recall@K, NDCG@K
- Tracking: MLflow

---

## Data Lineage & Reproducibility

- All datasets are versioned with DVC
- Git commits reference dataset versions via .dvc files
- Lineage is documented in docs/lineage.md
- Dataset update workflow is documented in docs/dvc_workflow.md

---

## Notes for Contributors

- Do not commit data folders (handled by DVC)
- Keep pipeline stages modular
- Run the Prefect flow for full reproducibility

---

## License / Usage

This project is intended for educational and internal use.
