from __future__ import annotations
from typing import Any, Dict
from pymongo.collection import Collection

from src.models.statistik import (
    StatistikGuruDocument,
    StatistikMuridDocument,
    StatistikSekolahDocument,
)

from .aggStatistikGuru import compute_StatistikGuru
from .aggStatistikMurid import compute_StatistikMurid
from .aggStatistikSekolah import compute_StatistikSekolah

STATISTIK_SEKOLAH_COLLECTION = "StatistikSekolah"
STATISTIK_GURU_COLLECTION = "StatistikGuru"
STATISTIK_MURID_COLLECTION = "StatistikMurid"


def compute_all_statistics(collection: Collection) -> Dict[str, Any]:
    return {
        "sekolah": compute_StatistikSekolah(collection),
        "guru": compute_StatistikGuru(collection),
        "murid": compute_StatistikMurid(collection),
    }


def build_statistik_documents(stats: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    """Convert aggregated stats into documents for Statistik collections."""

    sekolah_doc = (StatistikSekolahDocument(data=stats["sekolah"]["data"]).model_dump(by_alias=True))
    guru_doc = StatistikGuruDocument(data=stats["guru"]["data"]).model_dump(by_alias=True)
    murid_doc = StatistikMuridDocument(data=stats["murid"]["data"]).model_dump(by_alias=True)
    return {
        STATISTIK_SEKOLAH_COLLECTION: sekolah_doc,
        STATISTIK_GURU_COLLECTION: guru_doc,
        STATISTIK_MURID_COLLECTION: murid_doc,
    }


__all__ = [
    "compute_all_statistics",
    "compute_StatistikSekolah",
    "compute_StatistikGuru",
    "compute_StatistikMurid",
    "build_statistik_documents",
    "STATISTIK_SEKOLAH_COLLECTION",
    "STATISTIK_GURU_COLLECTION",
    "STATISTIK_MURID_COLLECTION",
]

