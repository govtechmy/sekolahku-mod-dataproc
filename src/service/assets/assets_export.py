"""Main asset export service for copying sekolah assets to public S3 bucket."""

from __future__ import annotations

import json
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from typing import Any

from botocore.exceptions import ClientError
from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.errors import PyMongoError

from src.config.settings import Settings
from src.core.aws import get_s3_client, get_s3_bucket_name
from .assets_path import (
    build_asset_source_path,
    build_asset_target_path,
    check_asset_exists,
    copy_s3_object,
    list_gallery_images,
    normalise_path_segment,
)

logger = logging.getLogger(__name__)


class AssetExportManifest:
    """Track asset export statistics and missing assets."""

    def __init__(self):
        self._lock = Lock()
        self.total_sekolah = 0
        self.sekolah_processed = 0
        self.logos_copied = 0
        self.heroes_copied = 0
        self.gallery_images_copied = 0
        self.missing_assets: list[dict[str, Any]] = []

    def increment(self, field: str, value: int = 1):
        """Thread-safe increment of a counter field."""
        with self._lock:
            setattr(self, field, getattr(self, field) + value)

    def add_missing_asset(
        self,
        kod_sekolah: str,
        nama_sekolah: str,
        negeri: str,
        parlimen: str,
        missing_types: list[str],
    ):
        """Thread-safe record of a sekolah with missing assets."""
        with self._lock:
            self.missing_assets.append({
                "kodSekolah": kod_sekolah,
                "namaSekolah": nama_sekolah,
                "negeri": negeri,
                "parlimen": parlimen,
                "missingAssets": missing_types,
            })

    def to_dict(self) -> dict[str, Any]:
        """Convert manifest to dictionary for JSON serialization."""
        return {
            "summary": {
                "totalSekolah": self.total_sekolah,
                "sekolahProcessed": self.sekolah_processed,
                "logosCopied": self.logos_copied,
                "heroesCopied": self.heroes_copied,
                "galleryImagesCopied": self.gallery_images_copied,
                "sekolahWithMissingAssets": len(self.missing_assets),
            },
            "missingAssets": self.missing_assets,
        }


def process_sekolah_assets(
    s3_client,
    sekolah_doc: dict[str, Any],
    source_bucket: str,
    target_bucket: str,
    source_prefix: str,
    manifest: AssetExportManifest,
) -> None:
    """
    Process and copy assets for a single sekolah.
    """
    kod_sekolah = sekolah_doc.get("kodSekolah")
    nama_sekolah = sekolah_doc.get("namaSekolah", "")
    data = sekolah_doc.get("data", {})
    pentadbiran = data.get("infoPentadbiran", {})

    negeri_raw = pentadbiran.get("negeri")
    parlimen_raw = pentadbiran.get("parlimen")

    # Normalize path segments
    negeri = normalise_path_segment(negeri_raw, "UNKNOWN_NEGERI")
    parlimen = normalise_path_segment(parlimen_raw, "UNKNOWN_PARLIMEN")

    if not kod_sekolah:
        logger.warning("Skipping sekolah with missing kodSekolah")
        return

    logger.debug(f"Processing assets for {kod_sekolah} ({nama_sekolah})")
    missing_types = []

    # --- Logo ---
    logo_source = build_asset_source_path(kod_sekolah, "logo", source_prefix)
    logo_target = build_asset_target_path(negeri, parlimen, kod_sekolah, "logo")
    if check_asset_exists(s3_client, source_bucket, logo_source):
        if copy_s3_object(s3_client, source_bucket, logo_source, target_bucket, logo_target):
            manifest.increment("logos_copied")
        else:
            missing_types.append("logo (copy failed)")
    else:
        missing_types.append("logo")

    # --- Hero ---
    hero_source = build_asset_source_path(kod_sekolah, "hero", source_prefix)
    hero_target = build_asset_target_path(negeri, parlimen, kod_sekolah, "hero")
    if check_asset_exists(s3_client, source_bucket, hero_source):
        if copy_s3_object(s3_client, source_bucket, hero_source, target_bucket, hero_target):
            manifest.increment("heroes_copied")
        else:
            missing_types.append("hero (copy failed)")
    else:
        missing_types.append("hero")

    # --- Gallery ---
    gallery_prefix = f"{source_prefix}/{kod_sekolah}/gallery"
    gallery_keys = list_gallery_images(s3_client, source_bucket, gallery_prefix)

    if gallery_keys:
        for idx, gallery_source in enumerate(gallery_keys):
            filename = gallery_source.split("/")[-1]
            target_filename = f"{idx}.jpg"  
            gallery_target = build_asset_target_path(
                negeri, parlimen, kod_sekolah, "gallery", target_filename
            )
            if copy_s3_object(s3_client, source_bucket, gallery_source, target_bucket, gallery_target):
                manifest.increment("gallery_images_copied")
        logger.debug(f"Copied {len(gallery_keys)} gallery images for {kod_sekolah}")
    else:
        missing_types.append("gallery")

    # --- Record missing assets ---
    if missing_types:
        manifest.add_missing_asset(kod_sekolah, nama_sekolah, negeri, parlimen, missing_types)

    # --- Increment sekolah processed ---
    manifest.increment("sekolah_processed")


def export_sekolah_assets(settings: Settings, status_filter: str = "ACTIVE") -> dict[str, Any]:
    """
    Export sekolah assets from source bucket to public bucket.
    """
    mongo_client = MongoClient(settings.mongo_uri, serverSelectionTimeoutMS=5000)
    s3_client = get_s3_client()

    source_bucket = settings.asset_export_source_bucket or settings.s3_bucket_dataproc
    target_bucket = get_s3_bucket_name()
    source_prefix = settings.s3_prefix_assets_source
    batch_size = settings.asset_export_batch_size
    max_workers = settings.asset_export_max_workers

    manifest = AssetExportManifest()

    try:
        collection: Collection = mongo_client[settings.db_name][settings.entiti_sekolah_collection]

        logger.info("=" * 60)
        logger.info("STARTING SEKOLAH ASSET EXPORT")
        logger.info(f"Source bucket: {source_bucket}")
        logger.info(f"Target bucket: {target_bucket}")
        logger.info(f"Source prefix: {source_prefix}")
        logger.info(f"Status filter: {status_filter}")
        logger.info(f"Collection: {settings.db_name}.{settings.entiti_sekolah_collection}")

        query_filter = {"status": status_filter} if status_filter else {}
        manifest.total_sekolah = collection.count_documents(query_filter)
        logger.info(f"Total sekolah to process: {manifest.total_sekolah}")

        cursor = collection.find(query_filter, {"_id": 0}).batch_size(batch_size)

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(
                process_sekolah_assets,
                s3_client,
                sekolah_doc,
                source_bucket,
                target_bucket,
                source_prefix,
                manifest
            ) for sekolah_doc in cursor]

            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    logger.error(f"Error processing sekolah assets: {e}")

        # --- Upload manifest ---
        manifest_data = manifest.to_dict()
        manifest_key = "common/asset-export-manifest.json"
        try:
            s3_client.put_object(
                Bucket=target_bucket,
                Key=manifest_key,
                Body=json.dumps(manifest_data, ensure_ascii=False, indent=2).encode("utf-8"),
                ContentType="application/json",
            )
            logger.info(f"Uploaded manifest to {target_bucket}/{manifest_key}")
        except ClientError as e:
            logger.error(f"Failed to upload manifest: {e}")

        # --- Log summary ---
        logger.info("=" * 60)
        logger.info("ASSET EXPORT COMPLETE")
        logger.info(f"Total sekolah: {manifest.total_sekolah}")
        logger.info(f"Sekolah processed: {manifest.sekolah_processed}")
        logger.info(f"Logos copied: {manifest.logos_copied}")
        logger.info(f"Hero images copied: {manifest.heroes_copied}")
        logger.info(f"Gallery images copied: {manifest.gallery_images_copied}")
        logger.info(f"Sekolah with missing assets: {len(manifest.missing_assets)}")

        return {
            "bucket": target_bucket,
            "manifest_key": manifest_key,
            **manifest_data["summary"],
        }

    except PyMongoError:
        logger.exception("MongoDB error during asset export")
        raise
    except ClientError:
        logger.exception("S3 error during asset export")
        raise
    finally:
        mongo_client.close()


__all__ = ["export_sekolah_assets"]
