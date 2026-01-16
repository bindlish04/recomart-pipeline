from src.feature_store.feature_store import FeatureStore

def main():
    fs = FeatureStore()

    print("Available feature views:", fs.list_feature_views())

    # Demo: retrieve user features for some users
    user_df = fs.get_features(
        view_name="user_features_v1",
        entity_ids=["U1", "U2", "U3"],
        features=["events_7d", "purchases_7d", "avg_price_7d", "last_event_ts"]
    )
    print("\nUser features:\n", user_df)

    # Demo: retrieve item features for some items
    item_df = fs.get_features(
        view_name="item_features_v1",
        entity_ids=["P10", "P11", "P12"],
        features=["views_7d", "carts_7d", "purchases_7d", "popularity_score_7d", "last_event_ts"]
    )
    print("\nItem features:\n", item_df)

if __name__ == "__main__":
    main()
