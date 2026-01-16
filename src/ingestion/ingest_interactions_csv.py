import pandas as pd
from datetime import datetime, timezone
from pathlib import Path
from src.common.logger import get_logger

logger = get_logger("ingest_csv")

INCOMING_DIR = Path("data/incoming")
RAW_BASE = Path("data/raw/interactions/source=csv")

def _partitions_now_utc():
    now = datetime.now(timezone.utc)
    return now.strftime("%Y-%m-%d"), now.strftime("%H")

def ingest_file(csv_path: Path) -> int:
    df = pd.read_csv(csv_path)
    rows = len(df)
    logger.info(f"Loaded {rows} rows from {csv_path}")

    date_part, hour_part = _partitions_now_utc()
    out_dir = RAW_BASE / f"date={date_part}" / f"hour={hour_part}"
    out_dir.mkdir(parents=True, exist_ok=True)

    out_path = out_dir / f"{csv_path.stem}.parquet"
    df.to_parquet(out_path, index=False)
    logger.info(f"Wrote raw parquet to {out_path}")
    return rows

def main():
    INCOMING_DIR.mkdir(parents=True, exist_ok=True)
    files = sorted(INCOMING_DIR.glob("*.csv"))

    if not files:
        logger.info("No CSV files found in data/incoming. Nothing to ingest.")
        return

    for f in files:
        try:
            ingest_file(f)
        except Exception as e:
            logger.exception(f"FAILED ingest for {f.name}: {e}")

if __name__ == "__main__":
    main()
