import sqlite3
import joblib
import pandas as pd
from datetime import datetime
from pathlib import Path

import mlflow

from src.common.logger import get_logger
from src.config import WAREHOUSE_DB, MODELS_DIR

logger = get_logger("train_model")

# Popularity weights (simple + explainable)
W_VIEW = 1
W_CART = 3
W_PURCHASE = 5

def load_tables():
    conn = sqlite3.connect(WAREHOUSE_DB)
    try:
        items = pd.read_sql_query("SELECT item_id, title, category, price FROM dim_items", conn)
        item_feat = pd.read_sql_query("SELECT * FROM features_item", conn)
        cooc = pd.read_sql_query("SELECT * FROM item_item_cooccurrence", conn)
        interactions = pd.read_sql_query("SELECT user_id, item_id, event_type, event_ts FROM fact_interactions", conn)
    finally:
        conn.close()
    return items, item_feat, cooc, interactions

def build_popularity(item_feat: pd.DataFrame) -> pd.DataFrame:
    # If popularity_score_7d exists (from Task 6), use it
    if "popularity_score_7d" in item_feat.columns:
        pop = item_feat[["item_id", "popularity_score_7d"]].copy()
        pop = pop.rename(columns={"popularity_score_7d": "popularity"})
        return pop.sort_values("popularity", ascending=False)

    # Fallback if needed
    item_feat = item_feat.fillna(0)
    item_feat["popularity"] = (
        item_feat.get("views_7d", 0) * W_VIEW +
        item_feat.get("carts_7d", 0) * W_CART +
        item_feat.get("purchases_7d", 0) * W_PURCHASE
    )
    return item_feat[["item_id", "popularity"]].sort_values("popularity", ascending=False)

def train_model():
    items, item_feat, cooc, interactions = load_tables()

    pop = build_popularity(item_feat)

    # Build a simple "neighbors" map from co-occurrence
    # Store for fast lookup: for each item A, list of (B, count)
    neighbors = {}
    for row in cooc.itertuples(index=False):
        a, b, c = row.item_id_a, row.item_id_b, int(row.cooc_count_30d)
        neighbors.setdefault(a, []).append((b, c))
        neighbors.setdefault(b, []).append((a, c))  # symmetric

    # Keep sorted neighbors
    for k in neighbors:
        neighbors[k].sort(key=lambda x: x[1], reverse=True)

    model = {
        "created_at_utc": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "popularity": pop,           # DataFrame: item_id, popularity
        "neighbors": neighbors,      # dict[item_id] -> list[(item_id, count)]
        "item_meta": items.set_index("item_id").to_dict(orient="index"),
        "weights": {"view": W_VIEW, "cart": W_CART, "purchase": W_PURCHASE},
    }

    return model

def main():
    MODELS_DIR.mkdir(parents=True, exist_ok=True)

    mlflow.set_experiment("recomart-recommender")

    with mlflow.start_run():
        # Log parameters
        mlflow.log_param("model_type", "popularity_plus_cooccurrence")
        mlflow.log_param("w_view", W_VIEW)
        mlflow.log_param("w_cart", W_CART)
        mlflow.log_param("w_purchase", W_PURCHASE)

        model = train_model()

        run_ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        model_path = MODELS_DIR / f"recomart_model_{run_ts}.pkl"
        joblib.dump(model, model_path)
        logger.info(f"Saved model to {model_path}")

        # Log artifact to MLflow
        mlflow.log_artifact(str(model_path))

        logger.info("Training completed successfully.")

if __name__ == "__main__":
    main()
