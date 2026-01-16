import json
import time
import requests
from datetime import datetime, timezone
from pathlib import Path
from src.common.logger import get_logger

logger = get_logger("ingest_api")

API_URL = "https://fakestoreapi.com/products"
RAW_BASE = Path("data/raw/products/source=api")

def _partitions_now_utc():
    now = datetime.now(timezone.utc)
    return now.strftime("%Y-%m-%d"), now.strftime("%H")

def fetch_with_retries(url: str, max_retries: int = 3, timeout_sec: int = 15) -> list:
    for attempt in range(1, max_retries + 1):
        try:
            r = requests.get(url, timeout=timeout_sec)
            if r.status_code == 200:
                return r.json()

            logger.warning(f"Attempt {attempt}: status={r.status_code}, body={r.text[:200]}")
        except Exception as e:
            logger.warning(f"Attempt {attempt} exception: {e}")

        time.sleep(2 * attempt)  # simple backoff

    raise RuntimeError(f"API fetch failed after {max_retries} retries: {url}")

def main():
    date_part, hour_part = _partitions_now_utc()
    out_dir = RAW_BASE / f"date={date_part}" / f"hour={hour_part}"
    out_dir.mkdir(parents=True, exist_ok=True)

    try:
        data = fetch_with_retries(API_URL)
        out_path = out_dir / "products.json"
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

        logger.info(f"Fetched {len(data)} products and saved to {out_path}")
    except Exception as e:
        logger.exception(f"FAILED API ingestion: {e}")

if __name__ == "__main__":
    main()
