"""
Process CSV assets:
- Read CSV from S3 or local with KOD_INSTITUSI, NAMA_PENUH_INSTITUSI, LOGO columns
- Match KOD_INSTITUSI with Sekolah collection _id
- Decode base64 logo to proper image format
- Upload to public S3 bucket: negeri/parlimen/kodSekolah/assets/logo.{ext}
- Store metadata in AssetSekolah collection with S3 URL
"""

from __future__ import annotations

import json
import logging
import pandas as pd
from collections import defaultdict
from typing import Dict, Optional, Set

from pymongo import MongoClient

from src.config.settings import Settings, get_settings
from src.core.s3 import _read_csv_from_s3, get_s3_client
from src.models.asset_sekolah import AssetSekolah, S3Urls
from src.service.assets.helpers import parse_image_data_url, _utc_now, build_manifest
from src.service.assets.logo_enum import LogoReason, LogoStatus


settings = get_settings()

logging.basicConfig(
    level=getattr(logging, settings.log_level.upper(), logging.INFO),
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)

logger = logging.getLogger(__name__)
s3 = get_s3_client()


def load_csv_logo_map(*, settings: Settings, sekolah_col) -> Dict[str, Optional[str]]:
    """
    Load CSV from S3 and return mapping:
    { kodSekolah -> base64 logo data or None }
    Only schools that exist in Sekolah collection are included.
    """

    # Preload all existing sekolah IDs once to avoid per-row DB lookups.
    existing_sekolah_ids: Set[str] = set(sekolah_col.distinct("_id"))

    s3_key = f"{settings.s3_prefix_assets}/{settings.asset_logo_csv_filename}"
    logger.info("Loading asset logo CSV from S3 in chunks: bucket=%s key=%s", settings.s3_bucket_dataproc, s3_key)

    s3_client = get_s3_client()
    response = s3_client.get_object(Bucket=settings.s3_bucket_dataproc, Key=s3_key)
    body = response["Body"]

    logo_map: Dict[str, Optional[str]] = {}
    total_rows = 0
    matched_rows = 0
    csv_upcoming_schools_not_in_db = 0

    for df_chunk in pd.read_csv(
        body,
        dtype=str,
        chunksize=settings.asset_logo_csv_batch_size,
        usecols=["KOD_INSTITUSI", "LOGO"], # only read necessary columns
    ):
        df_chunk = df_chunk.fillna("")

        rows = df_chunk.to_dict(orient="records")

        for row in rows:
            total_rows += 1

            kod_institusi = row.get("KOD_INSTITUSI")
            if not kod_institusi:
                continue

            kod_institusi = kod_institusi.strip()

            # Only consider schools that exist in DB. The CSV may contain upcoming schools not yet in DB and should be skipped. Business logic.
            if kod_institusi not in existing_sekolah_ids:
                csv_upcoming_schools_not_in_db += 1
                continue

            matched_rows += 1
            logo_data = row.get("LOGO")
            logo_map[kod_institusi] = logo_data.strip() if logo_data else None

        logger.debug("CSV logo map progress: %d rows scanned, %d matched to sekolah, current map size=%d", total_rows, matched_rows, len(logo_map))

    logger.info("Finished building logo map: %d sekolah entries | rows scanned=%d | matched=%d | csv_upcoming_schools_not_in_db=%d", len(logo_map), total_rows, matched_rows, csv_upcoming_schools_not_in_db)

    return logo_map


def upload_logo_to_s3(*, logo_data_url: str, negeri: str, parlimen: str, kod_sekolah: str, settings: Settings) -> str:
    """
    Decode base64 logo and upload to public S3.
    Returns public S3 URL.
    """
    ext, img_bytes = parse_image_data_url(logo_data_url)

    key = f"{negeri}/{parlimen}/{kod_sekolah}/assets/logo.{ext}"

    s3.put_object(
        Bucket=settings.s3_bucket_public,
        Key=key,
        Body=img_bytes,
        ContentType=f"image/{ext}",
    )

    return f"https://{settings.s3_bucket_public}.s3.amazonaws.com/{key}"


def process_single_sekolah(*, sekolah: dict, logo_map: Dict[str, Optional[str]], settings: Settings) -> AssetSekolah:
    """
    Process a single sekolah document:
    - optionally upload logo
    - always return AssetSekolah model
    """
    kod_sekolah = sekolah["_id"]
    status = sekolah.get("status")

    negeri = sekolah.get("negeri")
    parlimen = sekolah.get("parlimen")

    logo_url: Optional[str] = None
    logo_data = logo_map.get(kod_sekolah)

    # Upload only if all required fields exist
    if negeri and parlimen and logo_data:
        logo_url = upload_logo_to_s3(
            logo_data_url=logo_data,
            negeri=negeri,
            parlimen=parlimen,
            kod_sekolah=kod_sekolah,
            settings=settings,
        )

    return AssetSekolah(
        kodSekolah=kod_sekolah,
        status=status,
        s3Url=S3Urls(logo=logo_url),
    )


def process_csv_assets(settings: Settings) -> dict:
    """
    Orchestrate full pipeline:
    CSV -> lookup kodSekolah -> iterate Sekolah -> upload S3 -> upsert AssetSekolah
    """
    mongo = MongoClient(settings.mongo_uri)
    db = mongo[settings.db_name]

    sekolah_col = db[settings.sekolah_collection]
    assets_col = db[settings.asset_sekolah_collection]

    logo_map = load_csv_logo_map(settings=settings, sekolah_col=sekolah_col)

    uploaded = failed = skipped = 0
    skipped_reasons = defaultdict(int)

    total = 0

    cursor = sekolah_col.find({}).batch_size(settings.asset_export_batch_size)

    total_schools = sekolah_col.count_documents({})
    last_logged_percent = -1

    manifests = []  # collect per-sekolah manifest entries

    logger.info("Starting processing of %d sekolah for logo assets...", total_schools)

    for sekolah in cursor:
        total += 1
        kod_sekolah = sekolah["_id"]

        try:
            asset = process_single_sekolah(
                sekolah=sekolah,
                logo_map=logo_map,
                settings=settings,
            )

            logo_status: LogoStatus
            logo_reason: Optional[LogoReason] = None

            if asset.s3Url.logo:
                uploaded += 1
                logo_status = LogoStatus.UPLOADED
            else:
                skipped += 1

                if not logo_map.get(kod_sekolah):
                    logo_reason = LogoReason.NO_LOGO_IN_CSV
                elif not sekolah.get("negeri"):
                    logo_reason = LogoReason.MISSING_NEGERI
                elif not sekolah.get("parlimen"):
                    logo_reason = LogoReason.MISSING_PARLIMEN
                else:
                    logo_reason = LogoReason.UNKNOWN

                skipped_reasons[logo_reason.value] += 1
                logo_status = LogoStatus.SKIPPED

            # upsert asset record
            assets_col.update_one(
                {"_id": asset.kodSekolah},
                {"$set": asset.to_document()},
                upsert=True,
            )

            # build per-sekolah manifest entry and collect
            manifests.append(
                build_manifest(
                    sekolah=sekolah,
                    logo_status=logo_status,
                    logo_reason=logo_reason,
                    logo_url=asset.s3Url.logo,
                )
            )

            percent = int((total / total_schools) * 100)
            if percent != last_logged_percent and percent % 10 == 0:
                logger.debug("Progress: %d%% (%d/%d) | uploaded=%d skipped=%d failed=%d", percent, total, total_schools,uploaded, skipped, failed)
                last_logged_percent = percent

        except Exception as e:
            failed += 1
            logger.error("Failed processing %s: %s", kod_sekolah, e)

    overall_manifest = {
        "generatedAt": _utc_now().isoformat(),
        "totalSekolah": len(manifests),
        "sekolah": manifests,
    }

    manifest_key = "manifest.json"
    s3.put_object(
        Bucket=settings.s3_bucket_public,
        Key=manifest_key,
        Body=json.dumps(overall_manifest, ensure_ascii=False).encode("utf-8"),
        ContentType="application/json",
    )

    logger.info("Uploaded manifest.json to s3://%s/%s", settings.s3_bucket_public, manifest_key)

    logger.info("Completed Asset Logo processing.")
    logger.info("Total processed: %d | Uploaded logos: %d | Skipped: %d | Failed: %d", total, uploaded, skipped, failed)
    if skipped_reasons:
        logger.info("Skip reasons: %s", dict(skipped_reasons))

    return {
        "total_processed": total,
        "uploaded": uploaded,
        "skipped": skipped,
        "failed": failed,
        "skipped_breakdown": dict(skipped_reasons),
    }
