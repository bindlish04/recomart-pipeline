-- Dimension table for items (products)
CREATE TABLE IF NOT EXISTS dim_items (
  item_id TEXT PRIMARY KEY,
  title TEXT,
  category TEXT,
  price REAL,
  source_snapshot TEXT
);

-- Fact table for interactions (cleaned/prepared)
CREATE TABLE IF NOT EXISTS fact_interactions (
  interaction_id INTEGER PRIMARY KEY AUTOINCREMENT,
  user_id TEXT NOT NULL,
  item_id TEXT NOT NULL,
  event_type TEXT NOT NULL,
  event_ts TEXT NOT NULL,
  price REAL,
  FOREIGN KEY (item_id) REFERENCES dim_items(item_id)
);

-- User-level features (example: activity frequency, avg spend)
CREATE TABLE IF NOT EXISTS features_user (
  user_id TEXT PRIMARY KEY,
  events_7d INTEGER,
  purchases_7d INTEGER,
  avg_price_7d REAL,
  last_event_ts TEXT
);

-- Item-level features (example: popularity)
CREATE TABLE IF NOT EXISTS features_item (
  item_id TEXT PRIMARY KEY,
  views_7d INTEGER,
  carts_7d INTEGER,
  purchases_7d INTEGER,
  popularity_score_7d REAL,
  last_event_ts TEXT,
  FOREIGN KEY (item_id) REFERENCES dim_items(item_id)
);

-- Co-occurrence / similarity proxy table
CREATE TABLE IF NOT EXISTS item_item_cooccurrence (
  item_id_a TEXT NOT NULL,
  item_id_b TEXT NOT NULL,
  cooc_count_30d INTEGER NOT NULL,
  PRIMARY KEY (item_id_a, item_id_b)
);
