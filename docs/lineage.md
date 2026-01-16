# Data Lineage (Task 8)

## Pipeline Stages
1. Raw
   - data/raw/interactions/source=csv/date=.../hour=.../*.parquet
   - data/raw/products/source=api/date=.../hour=.../products.json

2. Validated
   - data/validated/interactions_validated_<ts>.parquet
   - data/validated/products_validated_<ts>.parquet
   - data/reports/validation_*.json

3. Prepared
   - data/prepared/interactions_prepared_<ts>.parquet
   - data/prepared/products_prepared_<ts>.parquet
   - data/reports/EDA_Summary_<ts>.md and plots

4. Features / Warehouse
   - data/warehouse/recomart.db (dim_items, fact_interactions, features_user, features_item, cooccurrence)
   - data/features/training_frame_<ts>.parquet

5. Model (Task 9)
   - data/models/... (to be produced)
   - MLflow runs with parameters and metrics

## Versioning
All data folders above are tracked in DVC. Each Git commit corresponds to a specific dataset version via *.dvc files.
