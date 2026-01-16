# recomart-pipeline
RecoMart – End-to-End ML Data Pipeline

This repository implements an end-to-end data management and machine learning pipeline for a recommendation system use case (“RecoMart”).
It demonstrates best practices in data ingestion, validation, preparation, feature engineering, feature stores, data versioning, model training, experiment tracking, and orchestration.

The project was built as part of the Data Management for Machine Learning assignment and is designed to be reproducible, modular, and production-style.

**📌High-level Architecture**
Ingestion (CSV + API)
        ↓
Raw Data Lake (partitioned)
        ↓
Validation & Profiling
        ↓
Preparation & EDA
        ↓
Feature Engineering + Warehouse (SQLite)
        ↓
Feature Store
        ↓
Model Training & Evaluation (MLflow)
        ↓
Orchestration (Prefect)

**📁 Repository Structure**
recomart-pipeline/
│
├── src/
│   ├── ingestion/        # CSV & API ingestion
│   ├── validation/       # Data quality checks + reports
│   ├── preparation/      # Cleaning & EDA
│   ├── transformation/   # Feature engineering + warehouse
│   ├── feature_store/    # Feature registry & retrieval
│   ├── modeling/         # Model training & evaluation
│   ├── orchestration/    # Prefect pipeline
│   ├── common/           # Shared utilities (logging)
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
├── .gitignore
├── .dvc/
└── .dvcignore

**⚙️ Prerequisites**
Python 3.9+
Git
(Optional) DVC remote access if you want to pull pre-generated data

**🧪 Setup Instructions
1️⃣ Clone the repository**
git clone (https://github.com/bindlish04/recomart-pipeline#)
cd recomart-pipeline

**2️⃣ Create and activate virtual environment**
Windows (PowerShell):
py -m venv .venv
.\.venv\Scripts\activate

**3️⃣ Install dependencies**
pip install -r requirements.txt

**📦 Data Handling (DVC)
Option A — Pull existing data (if you have access to DVC remote)**
dvc pull
**Option B — Regenerate data locally (no remote access needed)**
The pipeline will regenerate all data from ingestion onward.

**▶️ Run the Full Pipeline (Recommended)**
This runs everything end-to-end:
python -m src.orchestration.prefect_flow

Pipeline stages:
CSV & API ingestion
Data validation + Data Quality PDF
Data preparation + EDA
Feature engineering + warehouse materialization
Model training & evaluation
MLflow logging

**📊 View Experiment Tracking (MLflow)**
After running training/evaluation:
mlflow ui

Open the URL shown (usually http://127.0.0.1:5000) to view:
Parameters
Metrics (Precision@K, Recall@K, NDCG@K)
Model artifacts

**🧠 Feature Store Usage (Example)**
python -m src.feature_store.demo_retrieve_features

Demonstrates:
Feature registry
Online-style feature retrieval for users/items

**📈 Model Overview**
**Model type:** Popularity + Co-occurrence recommender
**Features:** User & item aggregates (7-day windows)
**Evaluation metrics:**
Precision@K
Recall@K
NDCG@K
**Tracking:** MLflow

This simple model is intentionally chosen to focus on data pipeline quality, not model complexity.

**🔁 Data Versioning & Lineage**

All pipeline datasets are tracked using DVC
Git commits reference dataset versions via .dvc files
Lineage is documented in docs/lineage.md
Dataset update workflow is documented in docs/dvc_workflow.md

**📄 Key Documentation**
Raw storage design: docs/storage_structure.md
Feature engineering logic: docs/feature_logic.md
Feature store design: docs/feature_store.md
DVC workflow: docs/dvc_workflow.md
Lineage tracking: docs/lineage.md
Orchestration: docs/orchestration.md
