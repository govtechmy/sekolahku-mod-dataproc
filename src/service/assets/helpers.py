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

# -----------------------------------------------------------------------------
# Base64 image helpers (CSV ingestion)
# -----------------------------------------------------------------------------

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

# -----------------------------------------------------------------------------
# Naming helpers
# -----------------------------------------------------------------------------

def normalise_negeri(name: str) -> str:
    """Normalise negeri name: uppercase and replace spaces with hyphens."""
    if not name:
        raise ValueError("Negeri name cannot be empty or None")
    return name.strip().upper().replace(" ", "-")


def normalise_parlimen(name: str) -> str:
    """Normalise parlimen name: uppercase and replace spaces with hyphens."""
    if not name:
        raise ValueError("Parlimen name cannot be empty or None")
    return name.strip().upper().replace(" ", "-")

# -----------------------------------------------------------------------------
# S3 helpers
# -----------------------------------------------------------------------------

def parse_s3_url(url: str) -> tuple[str, str]:
    """Extract bucket and key from public S3 URL."""
    prefix, key = url.split(".amazonaws.com/", 1)
    bucket = prefix.replace("https://", "").split(".s3")[0]
    return bucket, key

# -----------------------------------------------------------------------------
# Date/time helpers
# -----------------------------------------------------------------------------

def _utc_now() -> datetime:
    """Return timezone-aware UTC datetime."""
    return datetime.now(timezone.utc)

# -----------------------------------------------------------------------------
# Collection helpers
# -----------------------------------------------------------------------------

def chunked(data: Iterable[T], size: int) -> Generator[list[T], None, None]:
    """
    Yield successive chunks from an iterable.
    
    Args:
        data: Iterable to chunk
        size: Size of each chunk
        
    Yields:
        Lists of items, each with at most 'size' elements
    """
    batch = []
    for item in data:
        batch.append(item)
        if len(batch) == size:
            yield batch
            batch = []
    if batch:
        yield batch
