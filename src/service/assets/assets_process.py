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

# Increase CSV field size limit to handle large base64 images
csv.field_size_limit(sys.maxsize)


# -----------------------------------------------------------------------------
# Entry point
# -----------------------------------------------------------------------------

def process_csv_assets(settings: Settings, csv_path: str) -> dict:
    s3 = boto3.client("s3")
    mongo = MongoClient(settings.mongo_uri)

    sekolah_col = mongo[settings.db_name]["Sekolah"]
    assets_col = mongo[settings.db_name]["AssetSekolah"]

    rows = _load_csv(s3, csv_path)
    
    chunk_size = settings.asset_export_batch_size
    
    logger.info("=" * 20)
    logger.info(f"Starting CSV asset processing (batch size: {chunk_size})")

    uploaded = skipped = failed = 0
    skipped_reasons = []
    failed_reasons = []

    for chunk_num, chunk in enumerate(chunked(rows, chunk_size), 1):
        logger.info(f"Processing chunk {chunk_num} ({len(chunk)} rows)")
        
        for row in chunk:
            try:
                # Handle None or empty KOD_INSTITUSI
                kod_raw = row.get("KOD_INSTITUSI")
                if not kod_raw:
                    skipped += 1
                    skipped_reasons.append({"kodSekolah": "UNKNOWN", "reason": "Missing KOD_INSTITUSI in CSV"})
                    continue
                
                kod = kod_raw.strip()
                data_url = row.get("LOGO")

                if not data_url or not data_url.strip():
                    skipped += 1
                    skipped_reasons.append({"kodSekolah": kod, "reason": "No LOGO data in CSV"})
                    continue

                # Check if school exists in DB
                sekolah = sekolah_col.find_one({"_id": kod})
                if not sekolah:
                    skipped += 1
                    skipped_reasons.append({"kodSekolah": kod, "reason": "Not found in DB"})
                    continue
                
                # Check if school is ACTIVE
                if sekolah.get("status") != "ACTIVE":
                    skipped += 1
                    skipped_reasons.append({"kodSekolah": kod, "reason": "Not ACTIVE"})
                    continue

                # Validate required fields exist
                negeri_raw = sekolah.get("negeri")
                parlimen_raw = sekolah.get("parlimen")
                
                if not negeri_raw:
                    failed += 1
                    failed_reasons.append({"kodSekolah": kod, "reason": "Missing negeri in DB"})
                    continue
                
                if not parlimen_raw:
                    failed += 1
                    failed_reasons.append({"kodSekolah": kod, "reason": "Missing parlimen in DB"})
                    continue

                negeri = normalise_negeri(negeri_raw)
                parlimen = normalise_parlimen(parlimen_raw)

                ext, img_bytes = parse_image_data_url(data_url)

                key = f"{negeri}/{parlimen}/{kod}/assets/logo.{ext}"
                s3.put_object(
                    Bucket=settings.s3_bucket_public,
                    Key=key,
                    Body=img_bytes,
                    ContentType=f"image/{ext}",
                )

                url = f"https://{settings.s3_bucket_public}.s3.amazonaws.com/{key}"

                assets_col.update_one(
                    {"_id": kod},
                    {
                        "$set": {
                            "logo": url,
                            "gallery": [],
                            "hero": None,
                            "status": "ACTIVE",
                            "updatedAt": _utc_now(),
                        }
                    },
                    upsert=True,
                )

                uploaded += 1

            except Exception as e:
                failed += 1
                kod = row.get("KOD_INSTITUSI", "UNKNOWN")
                failed_reasons.append({"kodSekolah": kod, "reason": str(e)})
                logger.error(f"Failed processing {kod}: {str(e)}")
        
        logger.info(f"Chunk {chunk_num} complete: uploaded={uploaded}, skipped={skipped}, failed={failed}")

    # Log summary
    logger.info("=" * 20)
    logger.info(f"CSV ASSET PROCESSING COMPLETE")
    logger.info(f"Uploaded: {uploaded} | Skipped: {skipped} | Failed: {failed}")
    
    # Log skipped reasons
    if skipped_reasons:
        logger.info(f"SKIPPED SCHOOLS ({len(skipped_reasons)} total):")
        # Group by reason
        from collections import defaultdict
        by_reason = defaultdict(list)
        for item in skipped_reasons:
            by_reason[item["reason"]].append(item["kodSekolah"])
        
        # Write consolidated skipped.txt
        skipped_filename = "src/service/assets/skipped.txt"
        _write_consolidated_report(skipped_filename, "SKIPPED", by_reason)
        logger.info(f"  Full skipped report written to: {skipped_filename}")
        
        for reason, schools in by_reason.items():
            logger.info(f"  {reason}: {len(schools)} schools")
            if len(schools) <= 50:
                logger.info(f"    Schools: {', '.join(schools)}")
            else:
                logger.info(f"    Schools (first 50): {', '.join(schools[:50])}")
                logger.info(f"    ... and {len(schools) - 50} more")
    
    # Log failed reasons
    if failed_reasons:
        logger.info("=" * 20)
        logger.info(f"FAILED SCHOOLS ({len(failed_reasons)} total):")
        # Group by reason
        from collections import defaultdict
        by_reason = defaultdict(list)
        for item in failed_reasons:
            by_reason[item["reason"]].append(item["kodSekolah"])
        
        # Write consolidated failed.txt
        failed_filename = "src/service/assets/failed.txt"
        _write_consolidated_report(failed_filename, "FAILED", by_reason)
        logger.info(f"  Full failed report written to: {failed_filename}")
        
        for reason, schools in by_reason.items():
            logger.info(f"  {reason}: {len(schools)} schools")
            if len(schools) <= 50:
                logger.info(f"    Schools: {', '.join(schools)}")
            else:
                logger.info(f"    Schools (first 50): {', '.join(schools[:50])}")
                logger.info(f"    ... and {len(schools) - 50} more")


    return {
        "uploaded": uploaded,
        "skipped": skipped,
        "failed": failed,
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
        f.write(f"{report_type} SCHOOLS REPORT\n")
        f.write(f"Total schools: {total_schools}\n")
        f.write(f"Generated: {_utc_now().isoformat()}\n")
        f.write("=" * 80 + "\n\n")
        
        for reason, schools in reasons_dict.items():
            f.write(f"Reason: {reason}\n")
            f.write(f"Total schools: {len(schools)}\n")
            f.write("=" * 80 + "\n")
            
            for i, kod in enumerate(schools, 1):
                f.write(f"{i}. {kod}\n")
            
            f.write("\n" + "=" * 80 + "\n\n")
    
    logger.info(f"Wrote {total_schools} schools to {filename}")


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
