import csv
import os
from typing import Iterable, Dict, Any

def load(csv_path: str) -> Iterable[Dict[str, Any]]:
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"CSV not found: {csv_path}")
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            yield row
