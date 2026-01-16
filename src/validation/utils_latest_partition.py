from pathlib import Path

def latest_partition(base_dir: Path) -> Path:
    """
    Expects base_dir like: data/raw/interactions/source=csv/
    with subfolders date=YYYY-MM-DD/hour=HH/
    Returns the latest hour folder path.
    """
    if not base_dir.exists():
        raise FileNotFoundError(f"Base dir not found: {base_dir}")

    date_dirs = sorted([p for p in base_dir.glob("date=*") if p.is_dir()])
    if not date_dirs:
        raise FileNotFoundError(f"No date partitions found under: {base_dir}")

    latest_date = date_dirs[-1]
    hour_dirs = sorted([p for p in latest_date.glob("hour=*") if p.is_dir()])
    if not hour_dirs:
        raise FileNotFoundError(f"No hour partitions found under: {latest_date}")

    return hour_dirs[-1]
