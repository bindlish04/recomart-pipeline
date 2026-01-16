from pathlib import Path

DATA_DIR = Path("data")
RAW_DIR = DATA_DIR / "raw"
VALIDATED_DIR = DATA_DIR / "validated"
REPORTS_DIR = DATA_DIR / "reports"

INTERACTIONS_RAW = RAW_DIR / "interactions" / "source=csv"
PRODUCTS_RAW = RAW_DIR / "products" / "source=api"
PREPARED_DIR = DATA_DIR / "prepared"
FEATURES_DIR = DATA_DIR / "features"
WAREHOUSE_DIR = DATA_DIR / "warehouse"
WAREHOUSE_DB = WAREHOUSE_DIR / "recomart.db"
MODELS_DIR = DATA_DIR / "models"