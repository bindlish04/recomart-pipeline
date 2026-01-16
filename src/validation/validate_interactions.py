import json
import pandas as pd
from pathlib import Path
from datetime import datetime
from src.common.logger import get_logger
from src.config import INTERACTIONS_RAW, VALIDATED_DIR, REPORTS_DIR
from src.validation.utils_latest_partition import latest_partition

logger = get_logger("validate_interactions")

EXPECTED_SCHEMA = {
    "user_id": "object",
    "item_id": "object",
    "event_type": "object",
    "timestamp": "object",   # we validate parseability separately
    "price": "float64"
}

ALLOWED_EVENT_TYPES = {"view", "cart", "purchase"}
PRICE_MIN, PRICE_MAX = 0.0, 100000.0

def main():
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    VALIDATED_DIR.mkdir(parents=True, exist_ok=True)

    part_dir = latest_partition(INTERACTIONS_RAW)
    files = sorted(part_dir.glob("*.parquet"))
    if not files:
        raise FileNotFoundError(f"No parquet files in latest partition: {part_dir}")

    df = pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)
    logger.info(f"Loaded {len(df)} rows from {part_dir}")

    report = {}
    report["dataset"] = "interactions"
    report["partition"] = str(part_dir)
    report["row_count"] = int(len(df))
    report["columns"] = list(df.columns)

    # 1) Missing values
    nulls = df.isna().sum().to_dict()
    report["missing_values"] = {k: int(v) for k, v in nulls.items()}

    # 2) Duplicate entries (define your “duplicate key”)
    dup_key = ["user_id", "item_id", "event_type", "timestamp"]
    duplicates = int(df.duplicated(subset=dup_key).sum()) if all(c in df.columns for c in dup_key) else None
    report["duplicate_rows_on_key"] = {"key": dup_key, "count": duplicates}

    # 3) Schema mismatch (columns + dtypes)
    schema_issues = []
    for col, exp_dtype in EXPECTED_SCHEMA.items():
        if col not in df.columns:
            schema_issues.append(f"Missing column: {col}")
        else:
            # pandas dtype checks are not always perfect; good enough for assignment
            actual = str(df[col].dtype)
            if exp_dtype not in actual:
                schema_issues.append(f"Dtype mismatch for {col}: expected~{exp_dtype}, got {actual}")
    extra_cols = [c for c in df.columns if c not in EXPECTED_SCHEMA]
    if extra_cols:
        schema_issues.append(f"Unexpected columns: {extra_cols}")
    report["schema_issues"] = schema_issues

    # 4) Range/format checks
    # timestamp parseability
    ts_parse = pd.to_datetime(df["timestamp"], errors="coerce", utc=True) if "timestamp" in df.columns else None
    bad_ts = int(ts_parse.isna().sum()) if ts_parse is not None else None
    report["format_checks"] = {"bad_timestamps": bad_ts}

    # event types
    bad_events = int((~df["event_type"].isin(ALLOWED_EVENT_TYPES)).sum()) if "event_type" in df.columns else None
    report["range_checks"] = {
        "bad_event_types": bad_events,
        "price_out_of_range": int(((df["price"] < PRICE_MIN) | (df["price"] > PRICE_MAX)).sum()) if "price" in df.columns else None
    }

    # Save JSON report
    run_ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    out_json = REPORTS_DIR / f"validation_interactions_{run_ts}.json"
    out_json.write_text(json.dumps(report, indent=2), encoding="utf-8")
    logger.info(f"Wrote validation JSON report to {out_json}")

    # Save validated copy (optional but useful for later tasks)
    out_valid = VALIDATED_DIR / f"interactions_validated_{run_ts}.parquet"
    df.to_parquet(out_valid, index=False)
    logger.info(f"Wrote validated dataset to {out_valid}")

if __name__ == "__main__":
    main()
