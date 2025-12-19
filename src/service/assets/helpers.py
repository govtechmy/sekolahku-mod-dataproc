"""
Shared helpers for asset processing & export.
Used by process_csv_assets and export_sekolah_public.
"""
from __future__ import annotations

import base64
import mimetypes
from datetime import datetime, timezone
from typing import Tuple, Iterable, Generator, TypeVar

T = TypeVar("T")

def parse_image_data_url(data_url: str) -> Tuple[str, bytes]:
    """
    Parse data:image/<type>;base64,... string.
    Returns (extension, image_bytes)
    """
    if not data_url.startswith("data:image/") or ";base64," not in data_url:
        raise ValueError("Invalid base64 image data URL")

    header, encoded = data_url.split(",", 1)
    mime = header.split(";")[0].replace("data:", "")
    ext = mimetypes.guess_extension(mime) or ".png"

    return ext.lstrip("."), base64.b64decode(encoded)


def _utc_now() -> datetime:
    """Return timezone-aware UTC datetime."""
    return datetime.now(timezone.utc)

def build_manifest(
    *,
    sekolah: dict,
    logo_status: str,
    logo_reason: Optional[str],
    logo_url: Optional[str],
) -> dict:
    """
    Build per-sekolah manifest.json content.
    """
    return {
        "kodSekolah": sekolah["_id"],
        "status": sekolah.get("status"),
        "negeri": sekolah.get("negeri"),
        "parlimen": sekolah.get("parlimen"),
        "logo": {
            "status": logo_status,   # uploaded | skipped | failed
            "reason": logo_reason,
            "s3_url": logo_url,
        },
        "generatedAt": _utc_now().isoformat(),
    }