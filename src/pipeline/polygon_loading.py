"""Pipeline for loading polygon data into MongoDB collections."""
from __future__ import annotations

import json
import logging
import os
from typing import Any, Dict, List

from pymongo import MongoClient, ReplaceOne
from pymongo.collection import Collection
from shapely.geometry import shape, mapping
from shapely.validation import make_valid

from src.config.settings import Settings, get_settings
from src.models.polygon import NegeriPolygon, ParlimenPolygon
from src.polygons.polygon import (
    normalize_state_name,
    normalize_parliament_name,
)

logger = logging.getLogger(__name__)


EXTRACTED_NEGERI_DIR = "data_output/extracted_negeri"
EXTRACTED_PARLIMEN_DIR = "data_output/extracted_parlimen"


def _is_degenerate_ring(coords: List[List[float]], tolerance: float = 1e-8) -> bool:
    """Check if coordinate ring has fewer than 3 unique vertices."""
    if len(coords) < 3:
        return True
    
    unique = []
    for coord in coords:
        if not any(((coord[0] - u[0])**2 + (coord[1] - u[1])**2)**0.5 < tolerance for u in unique):
            unique.append(coord)
    
    return len(unique) < 3


def _filter_degenerate_loops(geometry: Dict[str, Any]) -> Dict[str, Any]:
    """Remove degenerate loops from geometry."""
    geom_type = geometry["type"]
    coords = geometry["coordinates"]
    
    if geom_type == "MultiPolygon":
        filtered = [[ring for ring in poly if not _is_degenerate_ring(ring)] 
                    for poly in coords]
        filtered = [poly for poly in filtered if poly]  # Remove empty polygons
        return {"type": "MultiPolygon", "coordinates": filtered} if filtered else geometry
    
    elif geom_type == "Polygon":
        filtered = [ring for ring in coords if not _is_degenerate_ring(ring)]
        return {"type": "Polygon", "coordinates": filtered} if filtered else geometry
    
    return geometry


def _repair_geometry(geometry: Dict[str, Any]) -> Dict[str, Any]:
    """Repair invalid GeoJSON geometry using Shapely."""
    try:
        # Filter degenerate loops and convert to Shapely
        geometry = _filter_degenerate_loops(geometry)
        if not geometry.get("coordinates"):
            return geometry
        
        geom = shape(geometry)
        
        # Repair if invalid
        if not geom.is_valid:
            geom = make_valid(geom)
            if not geom.is_valid:
                geom = geom.buffer(0)
        
        return mapping(geom)
    except Exception as e:
        logger.error(f"Geometry repair failed: {e}")
        return geometry


def _get_db(settings: Settings):
    """Get MongoDB database connection."""
    client = MongoClient(settings.mongo_uri)
    return client[settings.db_name]


def _chunked(iterable, size):
    """Yield successive chunks from iterable."""
    chunk = []
    for item in iterable:
        chunk.append(item)
        if len(chunk) >= size:
            yield chunk
            chunk = []
    if chunk:
        yield chunk


def _create_indexes(db, settings: Settings) -> Dict[str, Any]:
    """Create indexes for polygon collections."""
    logger.info("Creating indexes for polygon collections...")
    
    negeri_collection = db[settings.negeri_polygon_collection]
    parlimen_collection = db[settings.parlimen_polygon_collection]
    
    result = {
        "negeri": {"indexes": [], "errors": []},
        "parlimen": {"indexes": [], "errors": []}
    }
    
    # Define indexes to create
    indexes = [
        (negeri_collection, "negeri", [("geometry", "2dsphere")], "geometry_2dsphere"),
        (negeri_collection, "negeri", "negeri", "negeri_1"),
        (parlimen_collection, "parlimen", [("geometry", "2dsphere")], "geometry_2dsphere"),
        (parlimen_collection, "parlimen", [("negeri", 1), ("parlimen", 1)], "negeri_1_parlimen_1"),
        (parlimen_collection, "parlimen", "negeri", "negeri_1"),
    ]
    
    for collection, coll_key, index_spec, index_name in indexes:
        try:
            collection.create_index(index_spec, name=index_name, background=True)
            result[coll_key]["indexes"].append(index_name)
            logger.info(f"✓ Created {index_name}")
        except Exception as e:
            result[coll_key]["errors"].append(f"{index_name}: {str(e)}")
            logger.error(f"✗ Failed to create {index_name}: {e}")
    
    return result


def _load_negeri_polygons(extracted_dir: str) -> List[NegeriPolygon]:
    """Load state polygon data from extracted JSON files."""
    polygons: List[NegeriPolygon] = []
    
    if not os.path.exists(extracted_dir):
        logger.warning(f"Directory not found: {extracted_dir}")
        return polygons
    
    for filename in [f for f in os.listdir(extracted_dir) if f.endswith('.json')]:
        try:
            with open(os.path.join(extracted_dir, filename), 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            geojson = data.get('geojson', {})
            raw_state = geojson.get('properties', {}).get('state')
            geometry = geojson.get('geometry')
            
            if not raw_state or not geometry:
                continue
            
            negeri_enum = normalize_state_name(raw_state)
            if not negeri_enum:
                continue
            
            polygons.append(NegeriPolygon(
                negeri=negeri_enum,
                geometry=_repair_geometry(geometry)
            ))
            
        except Exception as e:
            logger.error(f"Error loading {filename}: {e}")
    
    logger.info(f"Loaded {len(polygons)} negeri polygons")
    return polygons


def _load_parlimen_polygons(extracted_dir: str) -> List[ParlimenPolygon]:
    """Load parliament polygon data from extracted JSON files."""
    polygons: List[ParlimenPolygon] = []
    
    if not os.path.exists(extracted_dir):
        logger.warning(f"Directory not found: {extracted_dir}")
        return polygons
    
    for filename in [f for f in os.listdir(extracted_dir) if f.endswith('.json') and '_P.' in f]:
        try:
            with open(os.path.join(extracted_dir, filename), 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            geojson = data.get('geojson', {})
            properties = geojson.get('properties', {})
            raw_state = properties.get('state')
            raw_parlimen = properties.get('parlimen')
            geometry = geojson.get('geometry')
            
            if not all([raw_state, raw_parlimen, geometry]):
                continue
            
            negeri_enum = normalize_state_name(raw_state)
            parlimen_normalized = normalize_parliament_name(raw_parlimen)
            
            if not negeri_enum or not parlimen_normalized:
                continue
            
            polygons.append(ParlimenPolygon(
                negeri=negeri_enum,
                parlimen=parlimen_normalized,
                geometry=_repair_geometry(geometry)
            ))
            
        except Exception as e:
            logger.error(f"Error loading {filename}: {e}")
    
    logger.info(f"Loaded {len(polygons)} parlimen polygons")
    return polygons


def _upsert_negeri_polygons(
    collection: Collection,
    polygons: List[NegeriPolygon],
    *,
    batch_size: int
) -> Dict[str, int]:
    """
    Upsert negeri polygons into MongoDB collection.
    
    Args:
        collection: MongoDB collection
        polygons: List of NegeriPolygon models
        batch_size: Batch size for bulk operations
        
    Returns:
        Dictionary with counts of processed, inserted, updated records
    """
    processed = 0
    inserted = 0
    updated = 0
    skipped = 0
    
    for chunk in _chunked(polygons, batch_size):
        operations: List[ReplaceOne] = []
        
        for polygon in chunk:
            try:
                processed += 1
                doc = polygon.to_document()
                _id = doc.get("_id")
                
                if _id is None:
                    skipped += 1
                    continue
                
                # Upsert: replace entire document (preserves field order)
                operations.append(
                    ReplaceOne(
                        {"_id": _id},
                        doc,
                        upsert=True
                    )
                )
                
            except Exception as e:
                logger.error(f"Error processing polygon {polygon.negeri}: {e}")
                skipped += 1
                continue
        
        if not operations:
            continue
        
        try:
            result = collection.bulk_write(operations, ordered=False)
            inserted += getattr(result, "upserted_count", 0) or 0
            updated += getattr(result, "modified_count", 0) or 0
        except Exception as e:
            logger.error(f"Error during bulk write: {e}")
            continue
    
    return {
        "processed": processed,
        "inserted": inserted,
        "updated": updated,
        "skipped": skipped,
    }


def _upsert_parlimen_polygons(
    collection: Collection,
    polygons: List[ParlimenPolygon],
    *,
    batch_size: int
) -> Dict[str, int]:
    """
    Upsert parlimen polygons into MongoDB collection.
    
    Args:
        collection: MongoDB collection
        polygons: List of ParlimenPolygon models
        batch_size: Batch size for bulk operations
        
    Returns:
        Dictionary with counts of processed, inserted, updated records
    """
    processed = 0
    inserted = 0
    updated = 0
    skipped = 0
    
    for chunk in _chunked(polygons, batch_size):
        operations: List[ReplaceOne] = []
        
        for polygon in chunk:
            try:
                processed += 1
                doc = polygon.to_document()
                _id = doc.get("_id")
                
                if _id is None:
                    skipped += 1
                    continue
                
                # Upsert: replace entire document (preserves field order)
                operations.append(
                    ReplaceOne(
                        {"_id": _id},
                        doc,
                        upsert=True
                    )
                )
                
            except Exception as e:
                logger.error(f"Error processing polygon {polygon.negeri}::{polygon.parlimen}: {e}")
                skipped += 1
                continue
        
        if not operations:
            continue
        
        try:
            result = collection.bulk_write(operations, ordered=False)
            inserted += getattr(result, "upserted_count", 0) or 0
            updated += getattr(result, "modified_count", 0) or 0
        except Exception as e:
            logger.error(f"Error during bulk write: {e}")
            continue
    
    return {
        "processed": processed,
        "inserted": inserted,
        "updated": updated,
        "skipped": skipped,
    }


def run_polygon_loading(settings: Settings | None = None) -> Dict[str, Any]:
    """
    Load polygon data from extracted JSONs into MongoDB collections.
    
    Args:
        settings: Settings object (uses defaults if None)
        
    Returns:
        Dictionary with summary statistics for both collections
    """
    if settings is None:
        settings = get_settings()
    
    logger.info("Starting polygon loading pipeline")
    
    # Connect to MongoDB
    db = _get_db(settings)
    negeri_collection = db[settings.negeri_polygon_collection]
    parlimen_collection = db[settings.parlimen_polygon_collection]
    
    # Load negeri polygons
    logger.info("Loading negeri polygons...")
    negeri_polygons = _load_negeri_polygons(EXTRACTED_NEGERI_DIR)
    
    # Load parlimen polygons
    logger.info("Loading parlimen polygons...")
    parlimen_polygons = _load_parlimen_polygons(EXTRACTED_PARLIMEN_DIR)
    
    # Build parlimen_list for each state
    logger.info("Building parlimen_list for each state...")
    negeri_to_parlimen: Dict[str, set] = {}
    for parlimen in parlimen_polygons:
        negeri_key = parlimen.negeri.value
        if negeri_key not in negeri_to_parlimen:
            negeri_to_parlimen[negeri_key] = set()
        negeri_to_parlimen[negeri_key].add(parlimen.parlimen)
    
    # Populate parlimen_list in negeri polygons (sorted, no duplicates)
    for negeri_polygon in negeri_polygons:
        negeri_key = negeri_polygon.negeri.value
        negeri_polygon.parlimen_list = sorted(list(negeri_to_parlimen.get(negeri_key, set())))
        logger.debug(f"State {negeri_key} has {len(negeri_polygon.parlimen_list)} parliaments")
    
    
    logger.info(f"Upserting {len(negeri_polygons)} negeri polygons...")
    negeri_result = _upsert_negeri_polygons(
        negeri_collection,
        negeri_polygons,
        batch_size=settings.batch_size,
    )
    
    logger.info(f"Upserting {len(parlimen_polygons)} parlimen polygons...")
    parlimen_result = _upsert_parlimen_polygons(
        parlimen_collection,
        parlimen_polygons,
        batch_size=settings.batch_size,
    )
    
    # Create indexes
    logger.info("Creating indexes...")
    indexes = _create_indexes(db, settings)
    
    summary = {
        "negeri": negeri_result,
        "parlimen": parlimen_result,
        "indexes": indexes,
    }
    
    logger.info("Polygon loading pipeline completed")
    logger.info(f"Negeri: {negeri_result}")
    logger.info(f"Parlimen: {parlimen_result}")
    logger.info(f"Indexes: {indexes}")
    
    return summary

# python -m src.main --load-polygons