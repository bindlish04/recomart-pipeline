import sqlite3
import pandas as pd
from datetime import datetime, timedelta, timezone
from pathlib import Path

from src.common.logger import get_logger
from src.config import PREPARED_DIR, FEATURES_DIR, WAREHOUSE_DIR, WAREHOUSE_DB
from src.preparation.utils_latest_file import latest_file

logger = get_logger("build_features")

SCHEMA_PATH = Path("src/transformation/warehouse_schema.sql")


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def ensure_no_duplicate_columns(df: pd.DataFrame, df_name: str):
    dupes = df.columns[df.columns.duplicated()].tolist()
    if dupes:
        raise ValueError(f"{df_name} has duplicate columns: {dupes}")


def main():
    FEATURES_DIR.mkdir(parents=True, exist_ok=True)
    WAREHOUSE_DIR.mkdir(parents=True, exist_ok=True)

    # 1) Load latest prepared datasets (Task 5 output)
    interactions_fp = latest_file(PREPARED_DIR, "interactions_prepared_*.parquet")
    products_fp = latest_file(PREPARED_DIR, "products_prepared_*.parquet")
    logger.info(f"Using prepared interactions: {interactions_fp}")
    logger.info(f"Using prepared products: {products_fp}")

    interactions = pd.read_parquet(interactions_fp)
    products = pd.read_parquet(products_fp)

    # Normalize key names: products has "id" from FakeStore; rename to item_id
    if "id" in products.columns and "item_id" not in products.columns:
        products = products.rename(columns={"id": "item_id"})

    # Convert timestamps for feature calculations
    if "timestamp" not in interactions.columns:
        raise ValueError("Prepared interactions must contain a 'timestamp' column.")
    interactions["timestamp"] = pd.to_datetime(interactions["timestamp"], utc=True, errors="coerce")

    # 2) Create / connect SQLite warehouse
    conn = sqlite3.connect(WAREHOUSE_DB)
    cur = conn.cursor()

    # 3) Create schema
    schema_sql = SCHEMA_PATH.read_text(encoding="utf-8")
    cur.executescript(schema_sql)
    conn.commit()
    logger.info(f"Initialized schema in {WAREHOUSE_DB}")

    # 4) Load dim_items
    needed_item_cols = ["item_id", "title", "category", "price"]
    missing_item_cols = [c for c in needed_item_cols if c not in products.columns]
    if missing_item_cols:
        raise ValueError(f"Prepared products missing columns: {missing_item_cols}")

    dim_items = products[needed_item_cols].copy()
    dim_items["source_snapshot"] = str(products_fp.name)
    ensure_no_duplicate_columns(dim_items, "dim_items")

    dim_items.to_sql("dim_items", conn, if_exists="replace", index=False)
    logger.info(f"Loaded dim_items: {len(dim_items)} rows")

    # 5) Load fact_interactions
    needed_fact_cols = ["user_id", "item_id", "event_type", "timestamp", "price"]
    missing_fact_cols = [c for c in needed_fact_cols if c not in interactions.columns]
    if missing_fact_cols:
        raise ValueError(f"Prepared interactions missing columns: {missing_fact_cols}")

    fact = interactions.rename(columns={"timestamp": "event_ts"}).copy()

    # Store timestamp as ISO string for SQLite
    fact["event_ts"] = pd.to_datetime(fact["event_ts"], utc=True, errors="coerce").dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    fact = fact[["user_id", "item_id", "event_type", "event_ts", "price"]]
    ensure_no_duplicate_columns(fact, "fact_interactions")

    fact.to_sql("fact_interactions", conn, if_exists="replace", index=False)
    logger.info(f"Loaded fact_interactions: {len(fact)} rows")

    # 6) Feature windows
    now = utc_now()
    t7 = now - timedelta(days=7)
    t30 = now - timedelta(days=30)

    # ---------- User features (7 days) ----------
    i7 = interactions[interactions["timestamp"] >= t7].copy()

    user_features = (
        i7.groupby("user_id", as_index=False)
          .agg(
              events_7d=("event_type", "count"),
              purchases_7d=("event_type", lambda s: int((s == "purchase").sum())),
              avg_price_7d=("price", "mean"),
              last_event_ts=("timestamp", "max"),
          )
    )

    user_features["avg_price_7d"] = pd.to_numeric(user_features["avg_price_7d"], errors="coerce").fillna(0.0)
    user_features["last_event_ts"] = pd.to_datetime(user_features["last_event_ts"], utc=True, errors="coerce") \
                                        .dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    ensure_no_duplicate_columns(user_features, "user_features")
    user_features.to_sql("features_user", conn, if_exists="replace", index=False)
    logger.info(f"Wrote features_user: {len(user_features)} rows")

    # ---------- Item features (7 days) ----------
    # Counts per event type
    views = i7[i7["event_type"] == "view"].groupby("item_id").size().rename("views_7d")
    carts = i7[i7["event_type"] == "cart"].groupby("item_id").size().rename("carts_7d")
    purchases = i7[i7["event_type"] == "purchase"].groupby("item_id").size().rename("purchases_7d")

    # Last event timestamp per item
    last_ts = (
        i7.groupby("item_id")["timestamp"]
          .max()
          .dt.strftime("%Y-%m-%dT%H:%M:%SZ")
          .rename("last_event_ts")
    )

    item_features = pd.concat([views, carts, purchases, last_ts], axis=1).fillna(0).reset_index()

    # Cast counts to int
    for c in ["views_7d", "carts_7d", "purchases_7d"]:
        item_features[c] = item_features[c].astype(int)

    # Weighted popularity score
    item_features["popularity_score_7d"] = (
        item_features["views_7d"] * 1
        + item_features["carts_7d"] * 3
        + item_features["purchases_7d"] * 5
    ).astype(float)

    ensure_no_duplicate_columns(item_features, "item_features")
    item_features.to_sql("features_item", conn, if_exists="replace", index=False)
    logger.info(f"Wrote features_item: {len(item_features)} rows")

    # ---------- Co-occurrence features (30 days) ----------
    i30 = interactions[interactions["timestamp"] >= t30].copy()
    user_items = (
        i30.groupby("user_id")["item_id"]
           .apply(lambda s: sorted(set(map(str, s))))
           .reset_index(name="items")
    )

    pairs = []
    for _, row in user_items.iterrows():
        items = row["items"]
        for i in range(len(items)):
            for j in range(i + 1, len(items)):
                pairs.append((items[i], items[j]))

    if pairs:
        pair_df = pd.DataFrame(pairs, columns=["item_id_a", "item_id_b"])
        cooc = pair_df.value_counts().reset_index(name="cooc_count_30d")
    else:
        cooc = pd.DataFrame(columns=["item_id_a", "item_id_b", "cooc_count_30d"])

    ensure_no_duplicate_columns(cooc, "cooc")
    cooc.to_sql("item_item_cooccurrence", conn, if_exists="replace", index=False)
    logger.info(f"Wrote item_item_cooccurrence: {len(cooc)} rows")

    # 7) Save a model-ready feature frame (optional but helpful for Task 9)
    training_frame = (
        interactions.merge(item_features, on="item_id", how="left")
                    .merge(user_features, on="user_id", how="left")
    )
    ensure_no_duplicate_columns(training_frame, "training_frame")

    run_ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    out_fp = FEATURES_DIR / f"training_frame_{run_ts}.parquet"
    training_frame.to_parquet(out_fp, index=False)
    logger.info(f"Wrote training frame parquet: {out_fp}")

    conn.close()
    logger.info("Task 6 completed successfully.")


if __name__ == "__main__":
    main()
