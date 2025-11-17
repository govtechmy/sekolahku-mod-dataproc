"""Statistics aggregation public API."""
from .aggregations import (
    compute_all_statistics,
    compute_StatistikSekolah,
    compute_StatistikGuru,
    compute_StatistikMurid,
    build_statistik_documents,
    STATISTIK_SEKOLAH_COLLECTION,
    STATISTIK_GURU_COLLECTION,
    STATISTIK_MURID_COLLECTION,
)

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
