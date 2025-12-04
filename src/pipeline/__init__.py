"""Pipeline package entrypoints."""
from .ingestion import run
from .entitiSekolah import run_entiti_sekolah, run_entiti_sekolah_dict
from .analitik import run_analitik_sekolah, run_analitik_dict
from .negeriParlimenKodSekolah import run_negeri_parlimen_kod_sekolah

__all__ = [
    "run",
    "run_entiti_sekolah",
    "run_entiti_sekolah_dict",
    "run_analitik_sekolah",
    "run_analitik_dict",
    "run_negeri_parlimen_kod_sekolah",
]