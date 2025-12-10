import json
import logging

from pymongo import MongoClient

from src.core import s3 as s3_core
from src.models.negeriEnum import NegeriEnum
from src.models.parlimenPolygon import ParlimenPolygon
from src.config.settings import get_settings

# --------------------------
# SETUP
# --------------------------
settings = get_settings()

logger = logging.getLogger(__name__)

# MongoDB client
mongo_client = MongoClient(settings.mongo_uri)
db = mongo_client[settings.db_name]
collection = db[settings.parlimen_polygon_collection]

# S3 client
s3_client = s3_core.get_s3_client()
bucket = settings.s3_bucket_dataproc
parlimen_prefix = f"{settings.s3_prefix_opendosm}raw/parlimen/"


# --------------------------
# NORMALIZE STATE NAME
# --------------------------
def normalize_state_name(raw_state: str) -> str | None:
    """
    Normalize state name from OpenDOSM format to NegeriEnum format.
    
    Handles special cases like:
    - "W.P. Kuala Lumpur" -> "WILAYAH_PERSEKUTUAN_KUALA_LUMPUR"
    - "Pulau Pinang" -> "PULAU_PINANG"
    - "Negeri Sembilan" -> "NEGERI_SEMBILAN"
    """
    # Special handling for Wilayah Persekutuan (W.P.)
    if raw_state.startswith("W.P."):
        # Remove "W.P." and add "WILAYAH_PERSEKUTUAN_"
        rest = raw_state.replace("W.P.", "").strip()
        normalized = f"WILAYAH_PERSEKUTUAN_{rest.upper().replace(' ', '_')}"
    else:
        # Regular normalization: uppercase and replace spaces
        normalized = raw_state.upper().replace(" ", "_")
    
    # Validate against enum
    try:
        NegeriEnum[normalized]
        return normalized
    except KeyError:
        return None


# --------------------------
# NORMALIZE PARLIAMENT NAME
# --------------------------
def normalize_parliament_name(raw_parlimen: str) -> str:
    """
    Normalize parliament name to uppercase with underscores.
    
    Example:
    - "Jempol" -> "JEMPOL"
    - "Kuala Lumpur" -> "KUALA_LUMPUR"
    """
    return raw_parlimen.upper().replace(" ", "_")


# --------------------------
# LIST JSON FILES IN S3
# --------------------------
def list_s3_json_files(bucket: str, prefix: str) -> list[str]:
    """Return list of JSON files in S3 under the given prefix."""
    keys = []
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith(".json"):
                keys.append(key)
    return keys


# --------------------------
# PROCESS FILES
# --------------------------
def main():
    parlimen_data = []

    # Load parliament geometries from parlimen files
    logger.info("=" * 60)
    logger.info("LOADING PARLIAMENT POLYGONS FROM S3")
    logger.info("=" * 60)
    parlimen_keys = list_s3_json_files(bucket, parlimen_prefix)
    if not parlimen_keys:
        logger.warning(f"No JSON files found in s3://{bucket}/{parlimen_prefix}")
        return

    skipped_count = 0
    processed_count = 0

    for key in parlimen_keys:
        logger.debug(f"Processing parlimen file: {key}")
        obj = s3_core.read_json_from_s3(bucket, key)  
        if not obj:
            logger.warning(f"Skipping empty or invalid JSON: {key}")
            skipped_count += 1
            continue

        pageProps = obj.get("pageProps", {})
        params = pageProps.get("params", {})
        geojson = pageProps.get("geojson", {})

        state_name = params.get("state")
        parlimen_id = params.get("id")  # e.g., "P.127 Jempol"
        geometry = geojson.get("geometry")

        if not state_name or not parlimen_id or not geometry:
            logger.warning(f"Skipping file {key} due to missing state, parlimen id, or geometry")
            skipped_count += 1
            continue
        
        # Extract parlimen name from id (e.g., "P.127 Jempol" -> "Jempol")
        parlimen_name = parlimen_id.split(" ", 1)[1] if " " in parlimen_id else parlimen_id

        # Normalize state name
        normalized_state = normalize_state_name(state_name)
        if not normalized_state:
            logger.warning(f"Skipping unknown negeri: {state_name} in {key}")
            skipped_count += 1
            continue
        
        negeri_enum = NegeriEnum[normalized_state]
        normalized_parlimen = normalize_parliament_name(parlimen_name)
        
        parlimen_data.append({
            'negeri': negeri_enum,
            'parlimen': normalized_parlimen,
            'geometry': geometry,
            'original_state': state_name,
            'original_parlimen': parlimen_name
        })
        
        processed_count += 1
        if processed_count % 50 == 0:
            logger.info(f"Processed {processed_count} parlimen files...")

    logger.info(f"\n✓ Total parlimen files processed: {processed_count}")
    logger.info(f"✓ Total parlimen files skipped: {skipped_count}")

    # --------------------------
    # UPSERT INTO MONGODB
    # --------------------------
    logger.info("\n" + "=" * 60)
    logger.info("UPSERTING TO MONGODB")
    logger.info("=" * 60)
    upserted_count = 0
    failed_count = 0
    
    for data in parlimen_data:
        try:
            model = ParlimenPolygon(
                negeri=data['negeri'],
                parlimen=data['parlimen'],
                geometry=data['geometry']
            )

            doc = model.to_document()
            collection.replace_one({"_id": doc["_id"]}, doc, upsert=True)
            logger.debug(f"Upserted {doc['_id']}")
            upserted_count += 1
            
            if upserted_count % 50 == 0:
                logger.info(f"Upserted {upserted_count} parliament records...")
                
        except Exception as e:
            failed_count += 1
            error_msg = str(e)
            parlimen_id = f"{data['negeri'].value}_{data['parlimen']}"
            
            # Extract key error info for MongoDB geometry validation errors
            if "Edges" in error_msg and "cross" in error_msg:
                # Extract just the edges crossing info
                error_summary = error_msg.split("Edge locations")[0].strip()
                logger.error(f"MongoDB rejected {parlimen_id}: {error_summary}")
            else:
                logger.error(f"Failed to upsert {parlimen_id}: {error_msg}")
            
            continue
    
    logger.info(f"\n✓ Total parlimen upserted: {upserted_count}")
    if failed_count > 0:
        logger.warning(f"Total parlimen failed: {failed_count}")

    # --------------------------
    # BUILD SUMMARY
    # --------------------------
    summary = {
        'parlimen': {
            'processed': processed_count,
            'succeeded': upserted_count,
            'failed': failed_count,
            'skipped': skipped_count,
            'collection': collection.name
        },
        'total_files_scanned': len(parlimen_keys)
    }
    
    logger.info(f"\nParlimen loading summary: {summary}")
    
    return summary


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    main()
