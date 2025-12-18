import json
import logging
from collections import defaultdict

from pymongo import MongoClient
from shapely.geometry import shape, mapping, Point
from shapely.validation import make_valid, explain_validity

from src.core import s3 as s3_core
from src.models.negeri_enum import NegeriEnum
from src.models.negeri_polygon import NegeriPolygon, NegeriPolygonCentroid
from src.config.settings import get_settings

# --------------------------
# SETUP
# --------------------------
settings = get_settings()
logger = logging.getLogger(__name__)

# MongoDB
mongo_client = MongoClient(settings.mongo_uri)
db = mongo_client[settings.db_name]
collection = db[settings.negeri_polygon_collection]
sekolah_collection = db[settings.sekolah_collection]

# S3
s3_client = s3_core.get_s3_client()
bucket = settings.s3_bucket_dataproc
negeri_prefix = f"{settings.s3_prefix_opendosm}/negeri/"

# --------------------------
# NORMALIZE STATE NAME
# --------------------------
def normalize_state_name(raw_state: str) -> str | None:
    if not raw_state:
        return None

    raw_state = raw_state.strip()

    # Special handling for Wilayah Persekutuan (W.P.)
    if raw_state.startswith("W.P."):
        rest = raw_state.replace("W.P.", "").strip()
        normalized = f"WILAYAH_PERSEKUTUAN_{rest.upper().replace(' ', '_')}"
    else:
        normalized = raw_state.upper().replace(" ", "_")

    try:
        NegeriEnum[normalized]
        return normalized
    except KeyError:
        return None


# --------------------------
# REPAIR INVALID GEOMETRY
# --------------------------
def repair_geometry(geometry: dict, state_name: str = "") -> dict:
    """
    Repair invalid geometries (e.g., self-intersecting polygons) using shapely.
    Returns the repaired geometry as a GeoJSON dict.
    - The SARAWAK polygon from OpenDOSM has self-intersecting edges (edges 122 and 124 cross
      at coordinates [2.4254100, 111.2836000] and [2.4254000, 111.2836200])
    - MongoDB's 2dsphere geospatial index validates GeoJSON geometries and rejects invalid ones
    - When trying to upsert SARAWAK without repair, MongoDB throws:
      "Can't extract geo keys... Edges 122 and 124 cross"
    """
    try:
        # skip heavy repair for other states
        if state_name not in ["SARAWAK", "SABAH"]:
            return geometry
        
        # Convert GeoJSON to shapely geometry
        geom = shape(geometry)

        # Check if geometry is valid
        if not geom.is_valid:
            reason = explain_validity(geom)
            logger.warning(f"Invalid geometry detected for {state_name}: {reason}")
            # Use shapely's make_valid to fix the geometry
            geom = make_valid(geom)
            logger.info(f"Geometry repaired successfully for {state_name}")

        # Convert back to GeoJSON
        return mapping(geom)

    except Exception as e:
        logger.error(f"Failed to repair geometry for {state_name}: {str(e)[:200]}")
        # Return original geometry if repair fails
        return geometry


# --------------------------
# LIST JSON FILES IN S3
# --------------------------
def list_s3_json_files(bucket: str, prefix: str) -> list[str]:
    keys = []
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith(".json"):
                keys.append(key)
    return keys


# --------------------------
# JSON EXTRACTORS
# --------------------------

def extract_state(obj: dict) -> str | None:
    page_props = obj.get("pageProps", {})
    params = page_props.get("params", {})
    properties = page_props.get("geojson", {}).get("properties", {})

    # State can be in params.state OR geojson.properties.state
    return params.get("state") or properties.get("state")


# --------------------------
# PROCESS FILES
# --------------------------


def main():
    negeri_to_geometry = {}

    # Load negeri geometries
    logger.info("[Negeri] Loading state geometries from S3 prefix '%s'", negeri_prefix)

    negeri_keys = list_s3_json_files(bucket, negeri_prefix)
    if not negeri_keys:
        logger.warning("[Negeri] No negeri JSON files found in s3://%s/%s", bucket, negeri_prefix)
        return

    logger.info("[Negeri] Found %d negeri JSON files", len(negeri_keys))

    for key in negeri_keys:
        logger.debug("[Negeri] Processing file: %s", key)
        obj = s3_core.read_json_from_s3(bucket, key)
        if not obj:
            logger.warning("[Negeri] Empty JSON: %s", key)
            continue

        state_name = extract_state(obj)
        geometry = obj.get("pageProps", {}).get("geojson", {}).get("geometry")

        if not state_name or not geometry:
            logger.warning("[Negeri] Missing state or geometry in %s", key)
            continue

        normalized = normalize_state_name(state_name)
        if not normalized:
            logger.warning("[Negeri] Unknown negeri name: %s", state_name)
            continue

        # Repair geometry if needed (fixes self-intersecting polygons)
        repaired_geometry = repair_geometry(geometry, normalized)

        negeri_to_geometry[NegeriEnum[normalized].value] = repaired_geometry

    logger.info("[Negeri] Loaded geometries for %d states", len(negeri_to_geometry))

    # UPSERT INTO MONGODB
    logger.info("[Negeri] Upserting state geometries to MongoDB collection '%s'", collection.name)

    upserted_count = 0
    failed_count = 0
    for negeri_str, geometry in negeri_to_geometry.items():
        negeri_enum = NegeriEnum[negeri_str]

        # Calculate centroid of schools in this negeri (GeoJSON + raw lon/lat)
        centroid_location, centroid_x, centroid_y = calculate_centroid(negeri_enum)

        centroid_obj: NegeriPolygonCentroid | None
        if centroid_location is not None and centroid_x is not None and centroid_y is not None:
            centroid_obj = NegeriPolygonCentroid(
                location=centroid_location,
                koordinatXX=centroid_x,
                koordinatYY=centroid_y,
            )
        else:
            centroid_obj = None

        model = NegeriPolygon(
            negeri=negeri_enum,
            geometry=geometry,
            centroid=centroid_obj,
        )
        try:
            collection.replace_one({"_id": negeri_str}, model.to_document(), upsert=True)
            upserted_count += 1
        except Exception as e:
            failed_count += 1
            logger.error("[Negeri] Failed to upsert negeri '%s' into collection '%s': %s", negeri_str, collection.name, str(e)[:500],)

    # --------------------------
    # SUMMARY
    # --------------------------
    summary = {
        "negeri": {
            "processed": len(negeri_to_geometry),
            "succeeded": upserted_count,
            "failed": failed_count,
            "collection": collection.name,
        },
        "total_negeri_files_scanned": len(negeri_keys),
    }

    logger.info("[Negeri] Summary: %s", summary)
    return summary


def calculate_centroid(negeri: NegeriEnum) -> tuple[dict | None, float | None, float | None]:
    """Calculate centroid of all schools in the given negeri.

    - Reads from Sekolah collection in MongoDB
    - Uses KOORDINATXX (x/longitude) and KOORDINATYY (y/latitude)
    - Returns GeoJSON Point {"type": "Point", "coordinates": [x, y]} or None
    """
    cursor = sekolah_collection.find(
        {
            "negeri": negeri.value,
            "location.type": "Point",
            "location.coordinates": {"$type": "array"},
        },
        {"location": 1},
    )

    total_lon = 0.0 # x
    total_lat = 0.0 # y
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

        total_lon += x # longitude
        total_lat += y # latitude
        count += 1

    if count == 0:
        logger.warning(f"No valid school coordinates found for negeri {negeri.value}; centroid will be None")
        return None, None, None

    center_lon = total_lon / count
    center_lat = total_lat / count

    # Shapely/GeoJSON convention: Point(lon, lat) -> [lon, lat]
    point = Point(center_lon, center_lat)
    return mapping(point), center_lon, center_lat


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    main()
