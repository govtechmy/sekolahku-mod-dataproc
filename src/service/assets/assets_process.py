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

import csv
import io
import logging
import sys
from collections import defaultdict

import boto3
from pymongo import MongoClient

from src.config.settings import Settings
from src.service.assets.helpers import (
    parse_image_data_url,
    normalise_negeri,
    normalise_parlimen,
    _utc_now,
    chunked,
)

logger = logging.getLogger(__name__)
if not logging.getLogger().handlers:
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )

# Increase CSV field size limit to handle large base64 images
csv.field_size_limit(sys.maxsize)


# -----------------------------------------------------------------------------
# Entry point
# -----------------------------------------------------------------------------

def process_csv_assets(settings: Settings, csv_path: str) -> dict:
    s3 = boto3.client("s3")
    mongo = MongoClient(settings.mongo_uri)
    
    db = mongo[settings.db_name]
    sekolah_col = db["Sekolah"]
    assets_col = db[settings.asset_sekolah_collection]

    rows = _load_csv(s3, csv_path)
    
    asset_export_batch_size = settings.asset_export_batch_size
    
    logger.debug("=" * 20)
    logger.debug(f"Starting CSV asset processing (batch size: {asset_export_batch_size})")

    uploaded = skipped = failed = 0
    skipped_reasons = []
    failed_reasons = []
    
    # Track logo URLs from CSV: {kodSekolah: logo_data_url}
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
            kod_sekolah = sekolah["_id"] if sekolah else None
            if not sekolah:
                skipped += 1
                skipped_reasons.append({"kod_institusi": kod_institusi, "kodSekolah": kod_sekolah, "reason": "KodSekolah in CSV but not in MongoDB (ignored)"})
                continue
            
            # Get LOGO data from CSV (can be null/empty)
            data_url = row.get("LOGO")
            if data_url:
                data_url = data_url.strip()
            
            # Store logo URL in map (even if empty/null)
            csv_logo_map[kod_institusi] = data_url if data_url else None
        
        logger.debug(f"Chunk {chunk_num} complete: Collected {len(csv_logo_map)} schools from CSV")
    
    # Now process ALL schools in MongoDB
    logger.debug("=" * 20)
    logger.debug("Processing all schools in MongoDB...")
    
    # Use cursor with batch_size for efficient streaming
    cursor = sekolah_col.find({}).batch_size(asset_export_batch_size)
    total_processed = 0
    
    logger.debug("Fetching documents from MongoDB: db=%s collection=%s", settings.db_name, "Sekolah")
    
    for sekolah in cursor:
        kod_sekolah = sekolah["_id"]
        status = sekolah.get("status", None)
        total_processed += 1
        
        try:
            # Get logo URL from CSV if available
            logo_data_url = csv_logo_map.get(kod_sekolah, None)
            
            # Validate required fields exist
            negeri_raw = sekolah.get("negeri")
            parlimen_raw = sekolah.get("parlimen")
            
            if not negeri_raw:
                failed += 1
                failed_reasons.append({"kodSekolah": kod_sekolah, "reason": "Missing negeri in DB"})
                continue
            
            if not parlimen_raw:
                failed += 1
                failed_reasons.append({"kodSekolah": kod_sekolah, "reason": "Missing parlimen in DB"})
                continue

            negeri = normalise_negeri(negeri_raw)
            parlimen = normalise_parlimen(parlimen_raw)
            
            # Prepare S3 URLs structure
            s3_urls = {
                "json": None,
                "logo": None,
                "gallery": None,
                "hero": None
            }
            
            # Process logo if available
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
                    
                    s3_urls["logo"] = f"https://{settings.s3_bucket_public}.s3.amazonaws.com/{logo_key}"
                    uploaded += 1
                    
                except Exception as e:
                    failed += 1
                    failed_reasons.append({"kodSekolah": kod_sekolah, "reason": f"Failed to upload logo: {str(e)}"})
                    logger.error(f"Failed processing logo for {kod_sekolah}: {str(e)}")

            # Update AssetSekolah with new structure
            assets_col.update_one(
                {"_id": kod_sekolah},
                {
                    "$set": {
                        "status": status,
                        "s3_urls": s3_urls,
                        "updatedAt": _utc_now(),
                    }
                },
                upsert=True,
            )
            
            # Log progress periodically
            if total_processed % 1000 == 0:
                logger.debug(f"Progress: Processed {total_processed} schools (uploaded={uploaded}, failed={failed})")

        except Exception as e:
            failed += 1
            failed_reasons.append({"kodSekolah": kod_sekolah, "reason": str(e)})
            logger.error(f"Failed processing {kod_sekolah}: {str(e)}")

    logger.debug(f"Processed all MongoDB schools: total={total_processed}, uploaded={uploaded}, failed={failed}")

    # Check for schools in DB but not in CSV
    logger.debug("Checking for schools in DB but not in CSV...")
    
    db_school_codes = set(school["_id"] for school in sekolah_col.find({}, {"_id": 1}))
    csv_school_codes = set(csv_logo_map.keys())
    
    db_not_in_csv = db_school_codes - csv_school_codes
    
    logger.debug(f"Total schools in DB: {len(db_school_codes)}")
    logger.debug(f"Total schools in CSV: {len(csv_school_codes)}")
    logger.debug(f"Schools in DB but NOT in CSV (logo set to null): {len(db_not_in_csv)}")
    
    if db_not_in_csv:
        # Log summary by status
        by_status = defaultdict(int)
        for kod_sekolah in db_not_in_csv:
            school = sekolah_col.find_one({"_id": kod_sekolah}, {"status": 1})
            if school:
                by_status[school.get("status", None)] += 1
        
        for status, count in by_status.items():
            logger.debug(f"  {status}: {count} schools")
    
    # Log summary
    logger.info(f"Successfully completed CSV asset processing")
    logger.info(f"Uploaded: {uploaded} | Skipped: {skipped} | Failed: {failed}")
    
    # Consolidate all skipped and failed reasons into error.txt
    all_reasons = []
    
    # Add skipped reasons
    if skipped_reasons:
        logger.debug(f"Skipped schools ({len(skipped_reasons)} total):")
        for item in skipped_reasons:
            all_reasons.append(item)
        
        # Group by reason for logging
        by_reason = defaultdict(list)
        for item in skipped_reasons:
            by_reason[item["reason"]].append(item["kodSekolah"])
        
        for reason, schools in by_reason.items():
            logger.debug(f"  {reason}: {len(schools)} schools")
            # Convert None to string for joining
            school_strs = [str(s) if s is not None else "None" for s in schools]
            if len(school_strs) <= 50:
                logger.debug(f"Schools: {', '.join(school_strs)}")
            else:
                logger.debug(f"Schools (first 50): {', '.join(school_strs[:50])}")
                logger.debug(f"... and {len(school_strs) - 50} more")
    
    # Add failed reasons
    if failed_reasons:
        logger.info(f" Not processed schools due to missing assets ({len(failed_reasons)} total):")
        for item in failed_reasons:
            all_reasons.append(item)
        
        # Group by reason for logging
        by_reason = defaultdict(list)
        for item in failed_reasons:
            by_reason[item["reason"]].append(item["kodSekolah"])
        
        len_limit = 50
        for reason, schools in by_reason.items():
            logger.debug(f"{reason}: {len(schools)} schools")
            # Convert None to string for joining
            school_strs = [str(s) if s is not None else "None" for s in schools]
            if len(school_strs) <= 50:
                logger.debug(f"Schools: {', '.join(school_strs)}")
            else:
                logger.debug(f"Schools (first {len_limit}): {', '.join(school_strs[:len_limit])} ... and {len(school_strs) - len_limit} more")

    # Write consolidated error.txt with all skipped and failed schools
    if all_reasons:
        by_reason = defaultdict(list)
        for item in all_reasons:
            by_reason[item["reason"]].append(item["kodSekolah"])
        
        error_filename = "src/service/assets/error.txt"
        _write_consolidated_report(error_filename, "ERROR", by_reason)

    return {
        "uploaded": uploaded,
        "skipped": skipped,
        "failed": failed,
        "total_schools_in_db": len(db_school_codes),
        "total_schools_in_csv": len(csv_school_codes),
        "db_not_in_csv": len(db_not_in_csv),
    }

# -----------------------------------------------------------------------------
# Internal helpers
# -----------------------------------------------------------------------------

def _write_consolidated_report(filename: str, report_type: str, reasons_dict: dict) -> None:
    """
    Write a consolidated report with all reasons and schools.
    
    Args:
        filename: Path to output file
        report_type: "SKIPPED" or "FAILED"
        reasons_dict: Dictionary mapping reason -> list of school codes
    """
    import os
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    
    total_schools = sum(len(schools) for schools in reasons_dict.values())
    
    with open(filename, "w", encoding="utf-8") as f:
        f.write(f"{report_type} School report\n")
        f.write(f"Total schools: {total_schools}\n")
        f.write(f"Generated: {_utc_now().isoformat()}\n")
        
        for reason, schools in reasons_dict.items():
            f.write(f"Reason: {reason}\n")
            f.write(f"Total schools: {len(schools)}\n")

            for i, kod_sekolah in enumerate(schools, 1):
                # Handle None values
                kod_str = str(kod_sekolah) if kod_sekolah is not None else "None"
                f.write(f"{i}. {kod_str}\n")

    logger.debug(f"Wrote {total_schools} schools to {filename}")


def _load_csv(s3, path: str):
    """
    Load CSV and return a DictReader for streaming.
    
    Returns an iterator that yields rows as dictionaries.
    This is more memory-efficient than loading the entire CSV into memory.
    """
    if path.startswith("s3://"):
        bucket, key = path.replace("s3://", "").split("/", 1)
        obj = s3.get_object(Bucket=bucket, Key=key)
        body = obj["Body"]
        stream = io.TextIOWrapper(body, encoding="utf-8")
    else:
        stream = open(path, "r", encoding="utf-8")

    return csv.DictReader(stream)
