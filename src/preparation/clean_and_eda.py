import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime
from pathlib import Path

from src.common.logger import get_logger
from src.config import VALIDATED_DIR, PREPARED_DIR, REPORTS_DIR
from src.preparation.utils_latest_file import latest_file

logger = get_logger("prep_eda")

def clean_interactions(df: pd.DataFrame) -> pd.DataFrame:
    # Basic column existence guard (beginner-friendly)
    expected_cols = ["user_id", "item_id", "event_type", "timestamp", "price"]
    missing = [c for c in expected_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Interactions missing columns: {missing}")

    # Drop rows with critical nulls
    before = len(df)
    df = df.dropna(subset=["user_id", "item_id", "event_type", "timestamp"])
    logger.info(f"Interactions: dropped {before - len(df)} rows due to null critical fields")

    # Normalize types
    df["user_id"] = df["user_id"].astype(str)
    df["item_id"] = df["item_id"].astype(str)
    df["event_type"] = df["event_type"].astype(str).str.lower().str.strip()

    # Timestamp parsing (invalid timestamps become NaT)
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce", utc=True)
    before = len(df)
    df = df.dropna(subset=["timestamp"])
    logger.info(f"Interactions: dropped {before - len(df)} rows with invalid timestamp")

    # Price cleaning
    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df["price"] = df["price"].fillna(0.0)
    df.loc[df["price"] < 0, "price"] = 0.0  # no negative prices

    # Deduplicate using business key
    before = len(df)
    df = df.drop_duplicates(subset=["user_id", "item_id", "event_type", "timestamp"])
    logger.info(f"Interactions: removed {before - len(df)} duplicate rows")

    return df

def clean_products(df: pd.DataFrame) -> pd.DataFrame:
    # FakeStore API uses id/title/price/category, but handle gracefully
    required = ["id", "title", "price", "category"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Products missing required columns: {missing}")

    # Basic cleaning
    df = df.dropna(subset=["id", "title", "category"])
    df["id"] = df["id"].astype(str)          # normalize key type
    df["title"] = df["title"].astype(str).str.strip()
    df["category"] = df["category"].astype(str).str.lower().str.strip()

    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df["price"] = df["price"].fillna(df["price"].median() if len(df) else 0.0)
    df.loc[df["price"] < 0, "price"] = 0.0

    # Deduplicate on product id
    before = len(df)
    df = df.drop_duplicates(subset=["id"])
    logger.info(f"Products: removed {before - len(df)} duplicate product IDs")

    return df

def save_plot(fig_path: Path):
    fig_path.parent.mkdir(parents=True, exist_ok=True)
    plt.tight_layout()
    plt.savefig(fig_path, dpi=150)
    plt.close()

def eda_interactions(df: pd.DataFrame, run_ts: str):
    # 1) Event type distribution
    plt.figure()
    df["event_type"].value_counts().plot(kind="bar")
    plt.title("Event Type Distribution")
    plt.xlabel("event_type")
    plt.ylabel("count")
    save_plot(REPORTS_DIR / f"eda_event_type_{run_ts}.png")

    # 2) Top items by interaction count
    plt.figure()
    top_items = df["item_id"].value_counts().head(10)
    top_items.plot(kind="bar")
    plt.title("Top 10 Items by Interactions")
    plt.xlabel("item_id")
    plt.ylabel("count")
    save_plot(REPORTS_DIR / f"eda_top_items_{run_ts}.png")

    # 3) Activity over time (daily)
    plt.figure()
    daily = df.set_index("timestamp").resample("D").size()
    daily.plot()
    plt.title("Interactions Over Time (Daily)")
    plt.xlabel("date")
    plt.ylabel("count")
    save_plot(REPORTS_DIR / f"eda_interactions_daily_{run_ts}.png")

def eda_products(df: pd.DataFrame, run_ts: str):
    # 1) Category distribution
    plt.figure()
    df["category"].value_counts().plot(kind="bar")
    plt.title("Product Category Distribution")
    plt.xlabel("category")
    plt.ylabel("count")
    save_plot(REPORTS_DIR / f"eda_product_categories_{run_ts}.png")

    # 2) Price distribution
    plt.figure()
    df["price"].plot(kind="hist", bins=20)
    plt.title("Product Price Distribution")
    plt.xlabel("price")
    plt.ylabel("frequency")
    save_plot(REPORTS_DIR / f"eda_product_prices_{run_ts}.png")

def write_eda_summary(interactions: pd.DataFrame, products: pd.DataFrame, run_ts: str):
    out_md = REPORTS_DIR / f"EDA_Summary_{run_ts}.md"
    lines = []
    lines.append("# EDA Summary (Task 5)\n")
    lines.append("## Interactions\n")
    lines.append(f"- Rows: {len(interactions)}")
    lines.append(f"- Unique users: {interactions['user_id'].nunique()}")
    lines.append(f"- Unique items: {interactions['item_id'].nunique()}")
    lines.append("- Event distribution:")
    lines.append(interactions["event_type"].value_counts().to_string())
    lines.append("\n## Products\n")
    lines.append(f"- Rows: {len(products)}")
    lines.append(f"- Unique categories: {products['category'].nunique()}")
    lines.append("- Category distribution:")
    lines.append(products["category"].value_counts().to_string())
    out_md.write_text("\n".join(lines), encoding="utf-8")
    logger.info(f"Wrote EDA summary: {out_md}")

def main():
    PREPARED_DIR.mkdir(parents=True, exist_ok=True)
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    interactions_file = latest_file(VALIDATED_DIR, "interactions_validated_*.parquet")
    products_file = latest_file(VALIDATED_DIR, "products_validated_*.parquet")

    logger.info(f"Using validated interactions: {interactions_file}")
    logger.info(f"Using validated products: {products_file}")

    interactions = pd.read_parquet(interactions_file)
    products = pd.read_parquet(products_file)

    interactions_clean = clean_interactions(interactions)
    products_clean = clean_products(products)

    run_ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")

    # Save prepared datasets
    out_i = PREPARED_DIR / f"interactions_prepared_{run_ts}.parquet"
    out_p = PREPARED_DIR / f"products_prepared_{run_ts}.parquet"
    interactions_clean.to_parquet(out_i, index=False)
    products_clean.to_parquet(out_p, index=False)
    logger.info(f"Wrote prepared interactions: {out_i}")
    logger.info(f"Wrote prepared products: {out_p}")

    # Run EDA + save plots
    eda_interactions(interactions_clean, run_ts)
    eda_products(products_clean, run_ts)
    write_eda_summary(interactions_clean, products_clean, run_ts)

    logger.info("Task 5 completed successfully.")

if __name__ == "__main__":
    main()
