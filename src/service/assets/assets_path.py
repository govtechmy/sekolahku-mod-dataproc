"""Helper functions for asset path management and S3 operations."""

from __future__ import annotations

import logging
from typing import Any, Optional

logger = logging.getLogger(__name__)


def build_asset_source_path(
    kod_sekolah: str,
    asset_type: str,
    source_prefix: str,
    gallery_index: Optional[int] = None,
) -> str:
    """
    Build the source S3 path for a sekolah asset.
    """
    if asset_type == "gallery" and gallery_index is not None:
        return f"{source_prefix}/{kod_sekolah}/gallery_{gallery_index}.jpg"
    return f"{source_prefix}/{kod_sekolah}/{asset_type}.jpg"


def build_asset_target_path(
    negeri: str,
    parlimen: str,
    kod_sekolah: str,
    asset_type: str,
    gallery_filename: Optional[str] = None,
) -> str:
    """
    Build the target S3 path for a sekolah asset in the public bucket.
    
    Target structure: negeri/parliament/sekolah_kod/assets/
    """
    base_path = f"{negeri}/{parlimen}/{kod_sekolah}/assets"
    
    if asset_type == "gallery" and gallery_filename:
        return f"{base_path}/gallery/{gallery_filename}"
    return f"{base_path}/{asset_type}.jpg"


def normalise_path_segment(value: Any, fallback: str = "UNKNOWN") -> str:
    """
    Normalize a path segment by replacing invalid characters.
    """
    text = (str(value).strip() if value else fallback).strip()
    if not text:
        text = fallback
    return text.replace("/", "-").replace(" ", "_").upper()


def check_asset_exists(s3_client, bucket: str, key: str) -> bool:
    """
    Check if an asset exists in S3.
    """
    try:
        s3_client.head_object(Bucket=bucket, Key=key)
        return True
    except s3_client.exceptions.NoSuchKey:
        return False
    except Exception as e:
        logger.warning(f"Error checking asset existence {bucket}/{key}: {e}")
        return False


def copy_s3_object(
    s3_client,
    source_bucket: str,
    source_key: str,
    target_bucket: str,
    target_key: str,
    content_type: str = "image/jpeg",
) -> bool:
    """
    Copy an object from source to target in S3.
    """
    try:
        copy_source = {"Bucket": source_bucket, "Key": source_key}
        s3_client.copy_object(
            CopySource=copy_source,
            Bucket=target_bucket,
            Key=target_key,
            ContentType=content_type,
            MetadataDirective="REPLACE",
        )
        logger.debug(f"Copied {source_bucket}/{source_key} -> {target_bucket}/{target_key}")
        return True
    except s3_client.exceptions.NoSuchKey:
        logger.debug(f"Source asset not found: {source_bucket}/{source_key}")
        return False
    except Exception as e:
        logger.error(f"Failed to copy asset: {e}")
        return False


def list_gallery_images(
    s3_client,
    bucket: str,
    prefix: str,
) -> list[str]:
    """
    List all gallery images for a sekolah in S3.
    """
    try:
        response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
        if "Contents" not in response:
            return []
        
        keys = [
            obj["Key"]
            for obj in response["Contents"]
            if obj["Key"].lower().endswith((".jpg", ".jpeg", ".png"))
        ]
        return sorted(keys)
    except Exception as e:
        logger.error(f"Failed to list gallery images: {e}")
        return []
