import sqlite3
import numpy as np
import pandas as pd
from datetime import datetime
import mlflow
import joblib

from src.common.logger import get_logger
from src.config import WAREHOUSE_DB, MODELS_DIR

logger = get_logger("evaluate")

K = 5
COOC_BOOST = 0.2  # how much to boost neighbor counts into ranking

def load_latest_model():
    models = sorted(MODELS_DIR.glob("recomart_model_*.pkl"))
    if not models:
        raise FileNotFoundError("No model found in data/models. Run training first.")
    return models[-1]

def load_interactions():
    conn = sqlite3.connect(WAREHOUSE_DB)
    try:
        df = pd.read_sql_query("SELECT user_id, item_id, event_type, event_ts FROM fact_interactions", conn)
    finally:
        conn.close()
    df["event_ts"] = pd.to_datetime(df["event_ts"], utc=True, errors="coerce")
    df = df.dropna(subset=["event_ts"])
    return df

def recommend(model, user_history_items, k=5):
    """
    Rank by popularity, then boost neighbors of recent items.
    """
    pop_df = model["popularity"].copy()
    pop_df["score"] = pop_df["popularity"].astype(float)

    neighbors = model["neighbors"]

    # Boost items that co-occur with user's history items
    boost = {}
    for it in user_history_items:
        for nbr, cnt in neighbors.get(it, [])[:50]:
            boost[nbr] = boost.get(nbr, 0) + cnt

    if boost:
        boost_series = pop_df["item_id"].map(lambda x: boost.get(x, 0)).astype(float)
        pop_df["score"] = pop_df["score"] + COOC_BOOST * boost_series

    # Remove items already in history
    hist = set(user_history_items)
    pop_df = pop_df[~pop_df["item_id"].isin(hist)]

    recs = pop_df.sort_values("score", ascending=False)["item_id"].head(k).tolist()
    return recs

def precision_recall_ndcg_at_k(recs, relevant_set, k):
    recs_k = recs[:k]
    if k == 0:
        return 0.0, 0.0, 0.0

    hits = [1 if r in relevant_set else 0 for r in recs_k]
    precision = sum(hits) / k
    recall = (sum(hits) / len(relevant_set)) if len(relevant_set) > 0 else 0.0

    # NDCG
    dcg = 0.0
    for i, hit in enumerate(hits, start=1):
        if hit:
            dcg += 1.0 / np.log2(i + 1)
    # ideal DCG
    ideal_hits = [1] * min(len(relevant_set), k)
    idcg = 0.0
    for i, hit in enumerate(ideal_hits, start=1):
        idcg += 1.0 / np.log2(i + 1)
    ndcg = (dcg / idcg) if idcg > 0 else 0.0

    return precision, recall, ndcg

def main():
    model_path = load_latest_model()
    model = joblib.load(model_path)
    logger.info(f"Loaded model: {model_path.name}")

    df = load_interactions()

    # Split: use each user's last event as "context" and any purchases as "relevant"
    # For tiny data, we keep it extremely simple.
    users = df["user_id"].unique().tolist()
    if len(users) == 0:
        raise ValueError("No interactions found to evaluate.")

    metrics = []
    for u in users:
        u_df = df[df["user_id"] == u].sort_values("event_ts")
        if len(u_df) < 2:
            continue

        # history = all but last
        history_items = u_df.iloc[:-1]["item_id"].astype(str).tolist()

        # relevant = last item if it was purchase; else treat last item as relevant anyway
        last_row = u_df.iloc[-1]
        if str(last_row["event_type"]).lower() == "purchase":
            relevant = {str(last_row["item_id"])}
        else:
            # fallback: next interaction target (still valid for ranking evaluation)
            relevant = {str(last_row["item_id"])}

        recs = recommend(model, history_items, k=K)
        p, r, n = precision_recall_ndcg_at_k(recs, relevant, K)
        metrics.append((p, r, n))

    if not metrics:
        raise ValueError("Not enough user history to evaluate (need at least 2 events per user).")

    p_mean = float(np.mean([m[0] for m in metrics]))
    r_mean = float(np.mean([m[1] for m in metrics]))
    n_mean = float(np.mean([m[2] for m in metrics]))

    # Log to MLflow
    mlflow.set_experiment("recomart-recommender")
    with mlflow.start_run():
        mlflow.log_param("k", K)
        mlflow.log_param("cooc_boost", COOC_BOOST)
        mlflow.log_metric("precision_at_k", p_mean)
        mlflow.log_metric("recall_at_k", r_mean)
        mlflow.log_metric("ndcg_at_k", n_mean)

        # Save a small report artifact
        report = (
            f"Model: {model_path.name}\n"
            f"K={K}, COOC_BOOST={COOC_BOOST}\n"
            f"precision@{K}: {p_mean:.4f}\n"
            f"recall@{K}: {r_mean:.4f}\n"
            f"ndcg@{K}: {n_mean:.4f}\n"
            f"evaluated_users: {len(metrics)}\n"
        )
        out_report = MODELS_DIR / f"model_eval_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.txt"
        out_report.write_text(report, encoding="utf-8")
        mlflow.log_artifact(str(out_report))

    logger.info("Evaluation complete.")
    logger.info(f"precision@{K}={p_mean:.4f}, recall@{K}={r_mean:.4f}, ndcg@{K}={n_mean:.4f}")

if __name__ == "__main__":
    main()
