"""Pipeline package entrypoints."""
from .ingestion import run
from .entiti_sekolah import run_entiti_sekolah, run_entiti_sekolah_dict
from .analitik_sekolah import run_analitik_sekolah, run_analitik_dict
from .negeri_parlimen_kod_sekolah import run_negeri_parlimen_kod_sekolah
from .institusi import run_institusi

__all__ = [
    "run",
    "run_entiti_sekolah",
    "run_entiti_sekolah_dict",
    "run_analitik_sekolah",
    "run_analitik_dict",
    "run_negeri_parlimen_kod_sekolah",
    "run_institusi",
]