from __future__ import annotations
from typing import Any, Dict
from pymongo.collection import Collection


def compute_StatistikGuru(collection: Collection) -> Dict[str, Any]:
    """Aggregate guru statistics for StatistikGuru collection."""
    pipeline = [
        {"$match": {"guru": {"$ne": None}}},
        {"$group": {"_id": None, "jumlahGuru": {"$sum": "$guru"}}},
    ]
    result = list(collection.aggregate(pipeline))
    total = result[0]["jumlahGuru"] if result else 0
    
    data = {
        "jumlahGuru": total,
        "jantina": {
            "perempuan": 50,
            "lelaki": 10,
            "tiada-maklumat": 0,
        },
    }

    return {"data": data}


__all__ = ["compute_StatistikGuru"]
