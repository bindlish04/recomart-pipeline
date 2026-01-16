# Feature Engineering Logic (Task 6)

## Inputs
- Prepared interactions (Parquet): user_id, item_id, event_type, timestamp, price
- Prepared products (Parquet): item_id, title, category, price

## Warehouse Storage
SQLite database: data/warehouse/recomart.db

Tables:
- dim_items: item metadata
- fact_interactions: cleaned interaction events
- features_user: user-level aggregates (7-day window)
- features_item: item-level aggregates (7-day window)
- item_item_cooccurrence: item-pair co-occurrence (30-day window)

## Features
### User features (7 days)
- events_7d: count of interactions in last 7 days
- purchases_7d: count of purchase events in last 7 days
- avg_price_7d: average price of events in last 7 days
- last_event_ts: most recent event timestamp

### Item features (7 days)
- views_7d, carts_7d, purchases_7d: event counts
- popularity_score_7d: weighted sum = 1*views + 3*carts + 5*purchases
- last_event_ts: most recent event timestamp

### Co-occurrence (30 days)
For each user, we take unique items interacted with in last 30 days and count item pairs (A,B).
High cooc_count_30d indicates items that co-occur frequently in user histories (similarity proxy).
