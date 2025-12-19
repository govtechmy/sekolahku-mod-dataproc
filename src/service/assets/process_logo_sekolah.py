"""
Process CSV assets:
- Read CSV from S3 or local with KOD_INSTITUSI, NAMA_PENUH_INSTITUSI, LOGO columns
- Match KOD_INSTITUSI with Sekolah collection _id
- Decode base64 logo to proper image format
- Upload to public S3 bucket: negeri/parlimen/kodSekolah/assets/logo.{ext}
- Store metadata in AssetSekolah collection with S3 URL
"""

from __future__ import annotations

import logging
from collections import defaultdict
from typing import Dict, Optional

from pymongo import MongoClient

from src.config.settings import Settings, get_settings
from src.core.s3 import _read_csv_from_s3, get_s3_client
from src.models.asset_sekolah import AssetSekolah, S3Urls
from src.service.assets.helpers import parse_image_data_url, _utc_now

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
    s3_key = f"{settings.s3_prefix_assets}/{settings.asset_logo_csv_filename}"
    logger.info("Loading asset logo CSV from S3: bucket=%s key=%s", settings.s3_bucket_dataproc, s3_key)

    df = _read_csv_from_s3(settings.s3_bucket_dataproc, s3_key)
    rows = df.to_dict(orient="records")
    rows = rows[:10]   # test only

    logo_map: Dict[str, Optional[str]] = {}

    for row in rows:
        kod_institusi = row.get("KOD_INSTITUSI")
        if not kod_institusi:
            continue

        kod_institusi = kod_institusi.strip()

        # Only consider schools that exist in DB
        if not sekolah_col.find_one({"_id": kod_institusi}, {"_id": 1}):
            continue

        logo_data = row.get("LOGO")
        logo_map[kod_institusi] = logo_data.strip() if logo_data else None

    logger.info("Loaded %d sekolah entries from CSV", len(logo_map))
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
        s3_urls=S3Urls(logo=logo_url),
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


    for sekolah in cursor:
        total += 1
        kod_sekolah = sekolah["_id"]

        try:
            asset = process_single_sekolah(
                sekolah=sekolah,
                logo_map=logo_map,
                settings=settings,
            )

            if asset.s3_urls.logo:
                uploaded += 1
            else:
                skipped += 1

                if not logo_map.get(kod_sekolah):
                    skipped_reasons["no_logo_in_csv"] += 1
                elif not sekolah.get("negeri"):
                    skipped_reasons["missing_negeri"] += 1
                elif not sekolah.get("parlimen"):
                    skipped_reasons["missing_parlimen"] += 1
                else:
                    skipped_reasons["unknown"] += 1

            assets_col.update_one(
                {"_id": asset.kodSekolah},
                {"$set": asset.to_document()},
                upsert=True,
            )

            if total % 1000 == 0:
                logger.info("Progress: %d processed | uploaded=%d failed=%d", total, uploaded, failed)

        except Exception as e:
            failed += 1
            logger.error("Failed processing %s: %s", kod_sekolah, e)

    logger.info("Completed Asset Logo processing.")
    logger.info("Total processed: %d", total)
    logger.info("Uploaded logos: %d | Skipped: %d | Failed: %d", uploaded, skipped, failed)

    return {
        "total_processed": total,
        "uploaded": uploaded,
        "skipped": skipped,
        "failed": failed,
        "skipped_breakdown": dict(skipped_reasons),
    }
