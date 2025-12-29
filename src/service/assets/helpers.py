"""
Shared helpers for asset processing & export.
Used by process_csv_assets and export_sekolah_public.
"""
from __future__ import annotations

import base64
import mimetypes
from datetime import datetime, timezone
from typing import Tuple, Optional

from src.service.assets.logo_enum import LogoStatus, LogoReason

from io import BytesIO
from PIL import Image
import base64


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
    logo_status: LogoStatus,
    logo_reason: Optional[LogoReason],
    logo_url: Optional[str],
) -> dict:
    """Build per-sekolah manifest.json content."""
    return {
        "kodSekolah": sekolah["_id"],
        "status": sekolah.get("status"),
        "negeri": sekolah.get("negeri"),
        "parlimen": sekolah.get("parlimen"),
        "logo": {
            "status": logo_status.value,   # uploaded | skipped | failed
            "reason": logo_reason.value if logo_reason is not None else None,
            "s3Url": logo_url,
        },
        "updatedAt": _utc_now().isoformat(),
    }

def convert_to_png(data_url: str) -> bytes:
    """
    Convert any base64 image to PNG bytes.
    """
    if not data_url.startswith("data:image/") or ";base64," not in data_url:
        raise ValueError("Invalid base64 image data URL")

    header, encoded = data_url.split(",", 1)
    img_bytes = base64.b64decode(encoded)

    with Image.open(BytesIO(img_bytes)) as img:
        img = img.convert("RGBA")
        output = BytesIO()
        img.save(output, format="PNG")
        return output.getvalue()
