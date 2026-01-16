import json
import pandas as pd
from pathlib import Path
from datetime import datetime
from src.common.logger import get_logger
from src.config import PRODUCTS_RAW, VALIDATED_DIR, REPORTS_DIR
from src.validation.utils_latest_partition import latest_partition

logger = get_logger("validate_products")

REQUIRED_FIELDS = ["id", "title", "price", "category"]

def main():
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    VALIDATED_DIR.mkdir(parents=True, exist_ok=True)

    part_dir = latest_partition(PRODUCTS_RAW)
    json_file = part_dir / "products.json"
    if not json_file.exists():
        raise FileNotFoundError(f"products.json not found in: {part_dir}")

    data = json.loads(json_file.read_text(encoding="utf-8"))
    df = pd.json_normalize(data)
    logger.info(f"Loaded {len(df)} rows from {json_file}")

    report = {
        "dataset": "products",
        "partition": str(part_dir),
        "row_count": int(len(df)),
        "columns": list(df.columns),
    }

    # Missing values
    nulls = df.isna().sum().to_dict()
    report["missing_values"] = {k: int(v) for k, v in nulls.items()}

    # Schema checks: required fields exist
    schema_issues = []
    for f in REQUIRED_FIELDS:
        if f not in df.columns:
            schema_issues.append(f"Missing required field: {f}")
    report["schema_issues"] = schema_issues

    # Duplicates: product id should be unique
    dup_ids = int(df.duplicated(subset=["id"]).sum()) if "id" in df.columns else None
    report["duplicate_rows_on_key"] = {"key": ["id"], "count": dup_ids}

    # Range checks: price should be non-negative and reasonable
    if "price" in df.columns:
        df["price"] = pd.to_numeric(df["price"], errors="coerce")
        report["range_checks"] = {
            "price_negative": int((df["price"] < 0).sum()),
            "price_null_after_cast": int(df["price"].isna().sum())
        }
    else:
        report["range_checks"] = {}

    run_ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    out_json = REPORTS_DIR / f"validation_products_{run_ts}.json"
    out_json.write_text(json.dumps(report, indent=2), encoding="utf-8")
    logger.info(f"Wrote validation JSON report to {out_json}")

    out_valid = VALIDATED_DIR / f"products_validated_{run_ts}.parquet"
    df.to_parquet(out_valid, index=False)
    logger.info(f"Wrote validated dataset to {out_valid}")

if __name__ == "__main__":
    main()
