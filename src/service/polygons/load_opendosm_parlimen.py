import json
import logging

from pymongo import MongoClient
from shapely.geometry import shape, mapping, Point
from shapely.validation import make_valid

from src.core import s3 as s3_core
from src.models.negeri_enum import NegeriEnum
from src.models.parlimen_polygon import ParlimenPolygon, ParlimenPolygonCentroid
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
sekolah_collection = db[settings.sekolah_collection]

# S3 client
s3_client = s3_core.get_s3_client()
bucket = settings.s3_bucket_dataproc
parlimen_prefix = f"{settings.s3_prefix_opendosm}/parlimen/"


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
# REPAIR INVALID GEOMETRY
# --------------------------
def repair_geometry(geometry: dict, parlimen_id: str = "") -> dict:
    """
    Repair invalid geometries (e.g., self-intersecting polygons) using shapely.
    Returns the repaired geometry as a GeoJSON dict.
    - MongoDB's 2dsphere geospatial index validates GeoJSON geometries and rejects invalid ones
    - This function fixes geometry issues like self-intersecting edges, invalid loops, etc.
    """
    try:
        from shapely.validation import explain_validity
        
        # Convert GeoJSON to shapely geometry
        geom = shape(geometry)
        
        # Check if geometry is valid
        if not geom.is_valid:
            reason = explain_validity(geom)
            logger.warning(f"Invalid geometry detected for {parlimen_id}: {reason}")
            # Use shapely's make_valid to fix the geometry
            geom = make_valid(geom)
            logger.info(f"Geometry repaired successfully for {parlimen_id}")
        
        # Convert back to GeoJSON
        return mapping(geom)
    except Exception as e:
        logger.error(f"Failed to repair geometry for {parlimen_id}: {str(e)[:200]}")
        # Return original geometry if repair fails
        return geometry


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


def calculate_centroid(negeri: NegeriEnum, parlimen: str) -> tuple[dict | None, float | None, float | None]:
    """Calculate centroid of all schools in the given negeri and parlimen.

    - Reads from Sekolah collection in MongoDB
    - Uses KOORDINATXX (x/longitude) and KOORDINATYY (y/latitude)
    - Returns GeoJSON Point {"type": "Point", "coordinates": [x, y]} or None
    """

    cursor = sekolah_collection.find(
        {
            "negeri": negeri.value,
            "parlimen": parlimen,
            "location.type": "Point",
            "location.coordinates": {"$type": "array"},
        },
        {"location": 1},
    )

    total_lon = 0.0
    total_lat = 0.0
    count = 0

    for doc in cursor:
        location = doc.get("location") or {}
        coordinates = location.get("coordinates")
        if not isinstance(coordinates, (list, tuple)) or len(coordinates) != 2:
            continue

        x, y = coordinates
        try:
            x = float(x)
            y = float(y)
        except (TypeError, ValueError):
            continue

        total_lon += x
        total_lat += y
        count += 1

    if count == 0:
        logger.warning(
            "[Parlimen] No valid school coordinates found for %s / %s; centroid will be None",
            negeri.value,
            parlimen,
        )
        return None, None, None

    center_lon = total_lon / count
    center_lat = total_lat / count

    point = Point(center_lon, center_lat)
    return mapping(point), center_lon, center_lat

def main():
    parlimen_data = []

    logger.info("[Parlimen] Loading parliament polygons from S3 prefix '%s'", parlimen_prefix)
    parlimen_keys = list_s3_json_files(bucket, parlimen_prefix)
    if not parlimen_keys:
        logger.warning("[Parlimen] No JSON files found in s3://%s/%s", bucket, parlimen_prefix)
        return

    logger.info("[Parlimen] Found %d parliament JSON files", len(parlimen_keys))

    skipped_count = 0
    processed_count = 0

    for key in parlimen_keys:
        logger.debug("[Parlimen] Processing parlimen file: %s", key)
        obj = s3_core.read_json_from_s3(bucket, key)
        if not obj:
            logger.warning("[Parlimen] Skipping empty or invalid JSON: %s", key)
            skipped_count += 1
            continue

        pageProps = obj.get("pageProps", {})
        params = pageProps.get("params", {})
        geojson = pageProps.get("geojson", {})

        state_name = params.get("state")
        parlimen_id = params.get("id")  # e.g., "P.127 Jempol"
        geometry = geojson.get("geometry")

        if not state_name or not parlimen_id or not geometry:
            logger.warning("[Parlimen] Skipping file %s due to missing state, parlimen id, or geometry", key)
            skipped_count += 1
            continue

        # Extract parlimen name from id (e.g., "P.127 Jempol" -> "Jempol")
        parlimen_name = parlimen_id.split(" ", 1)[1] if " " in parlimen_id else parlimen_id

        # Normalize state name
        normalized_state = normalize_state_name(state_name)
        if not normalized_state:
            logger.warning("[Parlimen] Skipping unknown negeri: %s in %s", state_name, key)
            skipped_count += 1
            continue

        negeri_enum = NegeriEnum[normalized_state]
        normalized_parlimen = normalize_parliament_name(parlimen_name)
        
        parlimen_id = f"{negeri_enum.value}::{normalized_parlimen}"
        repaired_geometry = repair_geometry(geometry, parlimen_id)

        parlimen_data.append({
            "negeri": negeri_enum,
            "parlimen": normalized_parlimen,
            "geometry": repaired_geometry,
            "original_state": state_name,
            "original_parlimen": parlimen_name,
        })

        processed_count += 1

    logger.info("[Parlimen] Loaded geometries for %d parliaments (skipped %d)", processed_count, skipped_count)

    logger.info("[Parlimen] Upserting parliament polygons to MongoDB collection '%s'", collection.name)
    upserted_count = 0
    failed_count = 0

    for data in parlimen_data:
        try:
            centroid_location, centroid_x, centroid_y = calculate_centroid(data["negeri"], data["parlimen"])

            centroid_obj: ParlimenPolygonCentroid | None
            if centroid_location is not None and centroid_x is not None and centroid_y is not None:
                centroid_obj = ParlimenPolygonCentroid(
                    location=centroid_location,
                    koordinatXX=centroid_x,
                    koordinatYY=centroid_y,
                )
            else:
                centroid_obj = None

            model = ParlimenPolygon(
                negeri=data["negeri"],
                parlimen=data["parlimen"],
                geometry=data["geometry"],
                centroid=centroid_obj,
            )

            doc = model.to_document()
            collection.replace_one({"_id": doc["_id"]}, doc, upsert=True)
            logger.debug("[Parlimen] Upserted %s", doc["_id"])
            upserted_count += 1

        except Exception as e:
            failed_count += 1
            error_msg = str(e)
            parlimen_id = f"{data['negeri'].value}_{data['parlimen']}"

            if "Edges" in error_msg and "cross" in error_msg:
                error_summary = error_msg.split("Edge locations")[0].strip()
                logger.error("[Parlimen] MongoDB rejected %s: %s", parlimen_id, error_summary)
            else:
                logger.error("[Parlimen] Failed to upsert %s: %s", parlimen_id, error_msg)

            continue

    # --------------------------
    # BUILD SUMMARY
    # --------------------------
    summary = {
        "parlimen": {
            "processed": processed_count,
            "succeeded": upserted_count,
            "failed": failed_count,
            "skipped": skipped_count,
            "collection": collection.name,
        },
        "total_files_scanned": len(parlimen_keys),
    }

    logger.info("[Parlimen] Summary: %s", summary)

    return summary


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    main()
