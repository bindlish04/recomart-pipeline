from pathlib import Path

def latest_file(dir_path: Path, pattern: str) -> Path:
    files = sorted(dir_path.glob(pattern))
    if not files:
        raise FileNotFoundError(f"No files found in {dir_path} matching {pattern}")
    return files[-1]
