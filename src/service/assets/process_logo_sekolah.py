"""
Process CSV assets:
- Read CSV from S3 or local with KOD_INSTITUSI, NAMA_PENUH_INSTITUSI, LOGO columns
- Match KOD_INSTITUSI with Sekolah collection _id
- Validate school status is ACTIVE
- Decode base64 logo to proper image format
- Upload to public S3 bucket: negeri/parlimen/kodSekolah/assets/logo.{ext}
- Store metadata in AssetSekolah collection with S3 URL
"""
from __future__ import annotations

from collections import defaultdict

from pymongo import MongoClient

from src.config.settings import Settings, get_settings
from src.core.s3 import _read_csv_from_s3, get_s3_client
from src.models.asset_sekolah import AssetSekolah, S3Urls
from src.service.assets.helpers import (parse_image_data_url, _utc_now, chunked)
import logging


settings = get_settings()

logging.basicConfig(
    level=getattr(logging, settings.log_level.upper(), logging.INFO),
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)

logger = logging.getLogger(__name__)
s3 = get_s3_client()

def process_csv_assets(settings: Settings) -> dict:
    """Process logo CSV from S3 into AssetSekolah collection.

    The CSV location is derived entirely from environment-driven settings:
    - S3_BUCKET_DATAPROC
    - s3_prefix_assets_source
    - ASSET_LOGO_CSV_FILENAME
    """

    mongo = MongoClient(settings.mongo_uri)
    
    db = mongo[settings.db_name]
    sekolah_col = db[settings.sekolah_collection]
    assets_col = db[settings.asset_sekolah_collection]

    s3_bucket_dataproc = settings.s3_bucket_dataproc
    s3_prefix_assets_source = settings.s3_prefix_assets_source
    asset_logo_csv_filename = settings.asset_logo_csv_filename

    s3_key = f"{s3_prefix_assets_source}/{asset_logo_csv_filename}" if s3_prefix_assets_source else asset_logo_csv_filename

    logger.info("Loading asset logo CSV from S3: bucket=%s key=%s", s3_bucket_dataproc, s3_key)

    df = _read_csv_from_s3(s3_bucket_dataproc, s3_key)
    rows = df.to_dict(orient="records")
    
    asset_export_batch_size = settings.asset_export_batch_size
    
    logger.debug("=" * 20)
    logger.debug(f"Starting CSV asset processing (batch size: {asset_export_batch_size})")

    uploaded = skipped = failed = 0
    skipped_reasons = []
    failed_reasons = []
    
    csv_logo_map = {}

    for chunk_num, chunk in enumerate(chunked(rows, asset_export_batch_size), 1):
        logger.debug(f"Processing chunk {chunk_num} - Reading CSV data")
        
        for row in chunk:
            # Handle None or empty KOD_INSTITUSI
            kod_raw = row.get("KOD_INSTITUSI")
            if not kod_raw:
                skipped += 1
                skipped_reasons.append({"kodSekolah": "UNKNOWN", "reason": "Missing KOD_INSTITUSI in CSV"})
                continue

            kod_institusi = kod_raw.strip()

            # Check if school exists in DB
            sekolah = sekolah_col.find_one({"_id": kod_institusi})
            if not sekolah:
                skipped += 1
                skipped_reasons.append({"kodSekolah": kod_institusi, "reason": "KodSekolah in CSV but not in MongoDB (ignored)"})
                continue
            
            # Get LOGO data from CSV (can be null/empty)
            data_url = row.get("LOGO")
            if data_url:
                data_url = data_url.strip()
            
            # Store logo URL in map (even if empty/null)
            csv_logo_map[kod_institusi] = data_url if data_url else None
        
        logger.debug(f"Chunk {chunk_num} complete: Collected {len(csv_logo_map)} sekolah from CSV")
        logger.debug("Processing all sekolah in MongoDB...")
    
    # Use cursor with batch_size for efficient streaming
    cursor = sekolah_col.find({}).batch_size(asset_export_batch_size)
    total_processed = 0
    
    logger.debug("Fetching documents from MongoDB: db=%s collection=%s", settings.db_name, settings.sekolah_collection)
    
    for sekolah in cursor:
        kod_sekolah = sekolah["_id"]
        status = sekolah.get("status", None)
        total_processed += 1
        
        try:
            # Get logo URL from CSV if available
            logo_data_url = csv_logo_map.get(kod_sekolah, None)
            
            # Get negeri and parlimen (already cleaned in Sekolah collection)
            negeri = sekolah.get("negeri")
            parlimen = sekolah.get("parlimen")
            
            # Check if negeri is missing - skip entirely
            if not negeri:
                failed += 1
                failed_reasons.append({"kodSekolah": kod_sekolah, "reason": "Missing negeri in DB"})
                continue
            
            # Prepare S3 URLs structure using model
            logo_url = None
            
            # If parlimen is missing, we can't upload to S3, but still create AssetSekolah record
            if not parlimen:
                skipped += 1
                skipped_reasons.append({"kodSekolah": kod_sekolah, "reason": "Missing parlimen in DB - S3 upload skipped"})
            else:
                if logo_data_url:
                    try:
                        ext, img_bytes = parse_image_data_url(logo_data_url)

                        logo_key = f"{negeri}/{parlimen}/{kod_sekolah}/assets/logo.{ext}"
                        s3.put_object(
                            Bucket=settings.s3_bucket_public,
                            Key=logo_key,
                            Body=img_bytes,
                            ContentType=f"image/{ext}",
                        )
                        
                        logo_url = f"https://{settings.s3_bucket_public}.s3.amazonaws.com/{logo_key}"
                        uploaded += 1
                        
                    except Exception as e:
                        failed += 1
                        failed_reasons.append({"kodSekolah": kod_sekolah, "reason": f"Failed to upload logo: {str(e)}"})
                        logger.error(f"Failed processing logo for {kod_sekolah}: {str(e)}")

            # Create AssetSekolah model and convert to document
            asset_sekolah = AssetSekolah(
                kodSekolah=kod_sekolah,
                status=status,
                s3_urls=S3Urls(logo=logo_url)
            )
            
            # Update AssetSekolah collection
            assets_col.update_one(
                {"_id": kod_sekolah},
                {"$set": asset_sekolah.to_document()},
                upsert=True,
            )
            
            if total_processed % 1000 == 0:
                logger.debug(f"Progress: Processed {total_processed} sekolah (uploaded={uploaded}, failed={failed})")

        except Exception as e:
            failed += 1
            failed_reasons.append({"kodSekolah": kod_sekolah, "reason": str(e)})
            logger.error(f"Failed processing {kod_sekolah}: {str(e)}")

    logger.debug(f"Processed all MongoDB sekolah: total={total_processed}, uploaded={uploaded}, failed={failed}")

    # Check for sekolah in DB but not in CSV
    logger.debug("Checking for sekolah in DB but not in CSV...")
    
    db_school_codes = set(school["_id"] for school in sekolah_col.find({}, {"_id": 1}))
    csv_school_codes = set(csv_logo_map.keys())
    
    db_not_in_csv = db_school_codes - csv_school_codes
    
    logger.debug(f"Total sekolah in DB: {len(db_school_codes)}")
    logger.debug(f"Total sekolah in CSV: {len(csv_school_codes)}")
    logger.debug(f"Schools in DB but NOT in CSV (logo set to null): {len(db_not_in_csv)}")
    
    if db_not_in_csv:
        by_status = defaultdict(int)
        for kod_sekolah in db_not_in_csv:
            school = sekolah_col.find_one({"_id": kod_sekolah}, {"status": 1})
            if school:
                by_status[school.get("status", None)] += 1
        
        for status, count in by_status.items():
            logger.debug(f"  {status}: {count} sekolah")
    
    # Log summary
    logger.info(f"Successfully completed CSV asset processing")
    logger.info(f"Uploaded: {uploaded} | Skipped: {skipped} | Failed: {failed}")
    logger.info(f"Total documents upserted to AssetSekolah collection: {total_processed}")

    all_reasons = []
    
    # Add skipped reasons
    if skipped_reasons:
        logger.debug(f"Skipped sekolah ({len(skipped_reasons)} total):")
        for item in skipped_reasons:
            all_reasons.append(item)

        by_reason = defaultdict(list)
        for item in skipped_reasons:
            by_reason[item["reason"]].append(item["kodSekolah"])
        
        for reason, sekolah in by_reason.items():
            logger.debug(f"  {reason}: {len(sekolah)} sekolah")
            school_strs = [str(s) if s is not None else "None" for s in sekolah]
            if len(school_strs) <= 50:
                logger.debug(f"Schools: {', '.join(school_strs)}")
            else:
                logger.debug(f"Schools (first 50): {', '.join(school_strs[:50])} ... and {len(school_strs) - 50} more")
    
    if failed_reasons:
        logger.info(f" Not processed sekolah due to missing assets ({len(failed_reasons)} total):")
        for item in failed_reasons:
            all_reasons.append(item)
        
        by_reason = defaultdict(list)
        for item in failed_reasons:
            by_reason[item["reason"]].append(item["kodSekolah"])
        
        len_limit = 50
        for reason, sekolah in by_reason.items():
            logger.debug(f"{reason}: {len(sekolah)} sekolah")
            school_strs = [str(s) if s is not None else "None" for s in sekolah]
            if len(school_strs) <= 50:
                logger.debug(f"Schools: {', '.join(school_strs)}")
            else:
                logger.debug(f"Schools (first {len_limit}): {', '.join(school_strs[:len_limit])} ... and {len(school_strs) - len_limit} more")

    if all_reasons:
        by_reason = defaultdict(list)
        for item in all_reasons:
            by_reason[item["reason"]].append(item["kodSekolah"])
        
        error_filename = "src/service/assets/error.txt"
        generate_summary(error_filename, "ERROR", by_reason)

    return {
        "uploaded": uploaded,
        "skipped": skipped,
        "failed": failed,
        "total_schools_in_db": len(db_school_codes),
        "total_schools_in_csv": len(csv_school_codes),
        "db_not_in_csv": len(db_not_in_csv),
    }


def generate_summary(filename: str, report_type: str, reasons_dict: dict) -> None:
    """Write a consolidated report with all reasons and sekolah."""
    import os
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    
    total_schools = sum(len(sekolah) for sekolah in reasons_dict.values())
    
    with open(filename, "w", encoding="utf-8") as f:
        f.write(f"{report_type} School report\n")
        f.write(f"Total sekolah: {total_schools}\n")
        f.write(f"Generated: {_utc_now().isoformat()}\n")
        
        for reason, sekolah in reasons_dict.items():
            f.write(f"Reason: {reason}\n")
            f.write(f"Total sekolah: {len(sekolah)}\n")

            for i, kod_sekolah in enumerate(sekolah, 1):
                kod_str = str(kod_sekolah) if kod_sekolah is not None else "None"
                f.write(f"{i}. {kod_str}\n")

    logger.debug(f"Wrote {total_schools} sekolah to {filename}")