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
