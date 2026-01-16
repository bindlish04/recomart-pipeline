import json
import sqlite3
from pathlib import Path
from typing import List, Optional
import pandas as pd

from src.common.logger import get_logger

logger = get_logger("feature_store")

REGISTRY_PATH = Path("src/feature_store/feature_registry.json")


class FeatureStore:
    def __init__(self, registry_path: Path = REGISTRY_PATH):
        self.registry = json.loads(registry_path.read_text(encoding="utf-8"))
        backend = self.registry["backend"]
        if backend["type"] != "sqlite":
            raise ValueError("This simple feature store only supports sqlite backend.")
        self.db_path = backend["db_path"]

    def _connect(self):
        return sqlite3.connect(self.db_path)

    def list_feature_views(self) -> List[str]:
        return [fv["name"] for fv in self.registry.get("feature_views", [])]

    def _get_view(self, view_name: str) -> dict:
        for fv in self.registry.get("feature_views", []):
            if fv["name"] == view_name:
                return fv
        raise ValueError(f"Feature view not found: {view_name}")

    def get_features(
        self,
        view_name: str,
        entity_ids: List[str],
        features: Optional[List[str]] = None,
        as_of_ts: Optional[str] = None
    ) -> pd.DataFrame:
        """
        Retrieve features for entities from a given feature view.
        - view_name: "user_features_v1" or "item_features_v1"
        - entity_ids: list of user_id or item_id
        - features: subset of feature columns; if None, returns all defined in registry
        - as_of_ts: optional ISO timestamp for "as of" filtering (best-effort, uses last_event_ts when present)
        """
        fv = self._get_view(view_name)
        table = fv["table"]
        entity_type = fv["entity"]
        pk = self.registry["entities"][entity_type]["primary_key"]

        # Determine columns to return
        defined = [f["name"] for f in fv["features"]]
        cols = defined if features is None else features

        # Always include primary key
        if pk not in cols:
            cols = [pk] + cols

        # Best-effort: ensure requested columns exist in registry definition
        for c in cols:
            if c != pk and c not in defined:
                raise ValueError(f"Feature '{c}' not in registry for {view_name}")

        placeholders = ",".join(["?"] * len(entity_ids))
        sql = f"SELECT {', '.join(cols)} FROM {table} WHERE {pk} IN ({placeholders})"

        # Optional as-of filtering if last_event_ts exists
        # (This is a simplified approach for the assignment.)
        if as_of_ts and "last_event_ts" in cols:
            sql += " AND last_event_ts <= ?"
            params = entity_ids + [as_of_ts]
        else:
            params = entity_ids

        conn = self._connect()
        try:
            df = pd.read_sql_query(sql, conn, params=params)
        finally:
            conn.close()

        # Ensure all requested entity_ids are represented (left-join behavior)
        # If some are missing, add rows with NaNs
        present = set(df[pk].astype(str).tolist()) if not df.empty else set()
        missing = [e for e in entity_ids if str(e) not in present]
        if missing:
            logger.warning(f"Missing entities in {view_name}: {missing}")
            missing_rows = pd.DataFrame({pk: missing})
            df = pd.concat([df, missing_rows], ignore_index=True, sort=False)

        return df
