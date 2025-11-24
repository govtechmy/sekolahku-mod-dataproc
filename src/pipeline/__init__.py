"""Pipeline package entrypoints."""
from .ingestion import run
from .entitiSekolah import run_entiti_sekolah, run_entiti_sekolah_dict

__all__ = [
    "run",
    "run_entiti_sekolah",
    "run_entiti_sekolah_dict",
]
