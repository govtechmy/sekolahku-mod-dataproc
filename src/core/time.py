from __future__ import annotations

from datetime import datetime, timezone


def _utc_now() -> datetime:
    """Return timezone UTC datetime."""
    return datetime.now(timezone.utc)

