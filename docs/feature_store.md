# Feature Store (Task 7)

## Purpose
A feature store ensures features used in training and inference are consistent, reusable, and versioned.

## Implementation
- Backend: SQLite (data/warehouse/recomart.db)
- Feature registry: src/feature_store/feature_registry.json
- Retrieval API: src/feature_store/feature_store.py

## Feature Views
- user_features_v1: features_user table (7-day aggregates)
- item_features_v1: features_item table (7-day aggregates + popularity)

## Retrieval
The `get_features()` method retrieves feature columns for a list of entity IDs.
A simplified "as-of" argument is supported via `last_event_ts` filtering when present.

## Why this meets Task 7
- Provides centralized feature definitions (registry + version)
- Supports online-style retrieval (by entity keys)
- Can be reused by training (Task 9) and inference pipelines
