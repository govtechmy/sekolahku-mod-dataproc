from __future__ import annotations

from enum import Enum


class LogoStatus(str, Enum):
    """Status of logo handling for a sekolah."""

    UPLOADED = "uploaded"
    SKIPPED = "skipped"
    FAILED = "failed"


class LogoReason(str, Enum):
    """Reason why a logo was skipped or not available."""

    NO_LOGO_IN_CSV = "no_logo_in_csv"
    MISSING_NEGERI = "missing_negeri"
    MISSING_PARLIMEN = "missing_parlimen"
    UNKNOWN = "unknown"
