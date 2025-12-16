from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Sequence, Set, Tuple
import os
import logging

import geopandas as gpd
import pandas as pd
from pymongo.collection import Collection
from shapely.geometry import mapping, shape
from shapely.geometry.base import BaseGeometry

from src.config.settings import get_settings
from src.core.db import get_mongo_client
from src.models.malaysia_polygon import Centroid, GeoJSONPoint, GeoJSONPolygon, MalaysiaPolygon
from src.models.negeriEnum import NegeriEnum

logger = logging.getLogger(__name__)
settings = get_settings()

WEST_MALAYSIA_STATES: Set[str] = {
    NegeriEnum.PERLIS.value,
    NegeriEnum.KEDAH.value,
    NegeriEnum.PULAU_PINANG.value,
    NegeriEnum.PERAK.value,
    NegeriEnum.KELANTAN.value,
    NegeriEnum.TERENGGANU.value,
    NegeriEnum.PAHANG.value,
    NegeriEnum.SELANGOR.value,
    NegeriEnum.NEGERI_SEMBILAN.value,
    NegeriEnum.MELAKA.value,
    NegeriEnum.JOHOR.value,
    NegeriEnum.WILAYAH_PERSEKUTUAN_KUALA_LUMPUR.value,
    NegeriEnum.WILAYAH_PERSEKUTUAN_PUTRAJAYA.value,
}


EAST_MALAYSIA_STATES: Set[str] = {
    NegeriEnum.SABAH.value,
    NegeriEnum.SARAWAK.value,
    NegeriEnum.WILAYAH_PERSEKUTUAN_LABUAN.value,
}


def _get_db():
    """Return a MongoDB database handle using configured DB name."""

    client = get_mongo_client()
    if not settings.db_name:
        raise ValueError("DB_NAME environment variable is not set")
    return client[settings.db_name]

def _mongo_geojson_to_shape(geojson: Dict[str, Any]) -> BaseGeometry:
    """Convert a GeoJSON geometry dict from MongoDB into a Shapely geometry."""

    if not geojson:
        raise ValueError("Missing geometry GeoJSON")

    try:
        return shape(geojson)
    except Exception as exc:
        raise ValueError(f"Invalid GeoJSON geometry: {exc}") from exc

def load_negeri_geodataframe(negeri_coll: Collection) -> gpd.GeoDataFrame:
    """Load all NegeriPolygon documents into a GeoDataFrame.

    The resulting GeoDataFrame has columns:
      - negeri (string)
      - geometry (Shapely geometry)
    """

    docs: List[Dict[str, Any]] = list(negeri_coll.find({}))
    if not docs:
        raise RuntimeError("No documents found in NegeriPolygon collection")

    records: List[Dict[str, Any]] = []
    for doc in docs:
        negeri = doc.get("negeri")
        geom_geojson = doc.get("geometry")

        if not negeri:
            raise ValueError(f"Document {doc.get('_id')} is missing 'negeri'")
        if not geom_geojson:
            raise ValueError(f"Document {doc.get('_id')} is missing 'geometry'")

        geom = _mongo_geojson_to_shape(geom_geojson)

        records.append({"negeri": negeri, "geometry": geom})

    gdf = gpd.GeoDataFrame(records, geometry="geometry", crs="EPSG:4326")
    return gdf


def dissolve_region(
    negeri_gdf: gpd.GeoDataFrame,
    negeri_names: Sequence[str] | Set[str],
    region_label: str,
) -> gpd.GeoDataFrame:
    """Filter states by name and dissolve them into a single region polygon to remove internal borders of each state."""

    negeri_set = {n.upper() for n in negeri_names}

    negeri_gdf_copy = negeri_gdf.copy()
    negeri_gdf_copy["negeri_upper"] = negeri_gdf_copy["negeri"].str.upper()

    region_gdf = negeri_gdf_copy[negeri_gdf_copy["negeri_upper"].isin(negeri_set)]
    if region_gdf.empty:
        raise RuntimeError(f"No negeri found for region {region_label}")

    region_gdf = region_gdf.assign(region=region_label)

    # Dissolve by region; merges polygons and drops internal borders
    dissolved = region_gdf.dissolve(by="region", as_index=False)

    if len(dissolved) != 1:
        raise RuntimeError(f"Expected a single dissolved geometry for region '{region_label}', but got {len(dissolved)}")

    return dissolved[["region", "geometry"]]


def _calculate_centroids_from_polygons(regions_gdf: gpd.GeoDataFrame) -> gpd.GeoSeries:
    """
    Compute an accurate centroid for a polygon or multipolygon.
    EPSG:4326 -> EPSG:3857 -> centroid -> EPSG:4326

    Workflow:
      1. Convert the geometry into a GeoDataFrame (EPSG:4326)
      2. Reproject to EPSG:3857 (meters) for accurate geometric calculations
      3. Compute centroid in EPSG:3857
      4. Reproject centroid back to EPSG:4326 for GeoJSON storage

    Returns:
        A Shapely Point geometry in EPSG:4326 (lon, lat).
    """

    # Project to Web Mercator for accurate centroid
    projected = regions_gdf.to_crs("EPSG:3857")
    projected_centroids = projected.geometry.centroid

    # Convert back to lat/lon for MongoDB storage
    centroids = projected_centroids.to_crs("EPSG:4326")
    return centroids


def build_region_polygons(negeri_gdf: gpd.GeoDataFrame) -> Iterable[MalaysiaPolygon]:
    """Create MalaysiaPolygon models for West and East Malaysia.

    1. Dissolve West Malaysia states.
    2. Dissolve East Malaysia states.
    3. Compute centroids.
    4. Build MalaysiaPolygon models.
    """

    west_gdf = dissolve_region(negeri_gdf, negeri_names=WEST_MALAYSIA_STATES, region_label="WEST_MALAYSIA")
    east_gdf = dissolve_region(negeri_gdf, negeri_names=EAST_MALAYSIA_STATES, region_label="EAST_MALAYSIA")

    combined = gpd.GeoDataFrame(pd.concat([west_gdf, east_gdf], ignore_index=True), crs=negeri_gdf.crs)

    centroids = _calculate_centroids_from_polygons(combined)

    now = datetime.now(timezone.utc)

    for idx, row in combined.iterrows():
        centroid_geom = centroids.iloc[idx]

        # Boundary polygon as GeoJSONPolygon
        geom_geojson = mapping(row["geometry"])
        boundary = GeoJSONPolygon(
            type=geom_geojson["type"],
            coordinates=geom_geojson["coordinates"],
        )

        # Centroid as structured Centroid with coordinates and GeoJSON location
        x, y = mapping(centroid_geom)["coordinates"]
        location = GeoJSONPoint(coordinates=(float(x), float(y)))
        centroid = Centroid(
            location=location,
            koordinatXX=float(x),
            koordinatYY=float(y),
        )

        yield MalaysiaPolygon(
            region=row["region"],
            geometry=boundary,
            centroid=centroid,
            createdAt=now,
        )


def persist_malaysia_polygons(region_polygons: Iterable[MalaysiaPolygon]) -> None:
    """Insert or update region polygons into MalaysiaPolygon collection."""
    db = _get_db()
    coll: Collection = db[settings.malaysia_polygon_collection]

    models: list[MalaysiaPolygon] = list(region_polygons)

    if not models:
        raise ValueError("No region polygons to persist")

    for m in models:
        doc = m.to_document()
        doc["_id"] = m.region

        coll.update_one(
            {"_id": doc["_id"]},
            {
                "$set": {
                    "geometry": doc["geometry"],
                    "centroid": doc["centroid"],
                },
                "$currentDate": {
                    "updatedAt": True
                },
                "$setOnInsert": {
                    "region": doc["region"],
                    "createdAt": doc["createdAt"],
                },
            },
            upsert=True,
        )


def run_malaysia_polygon_pipeline() -> None:
    """End-to-end pipeline entry point."""
    db = _get_db()
    negeri_coll: Collection = db[settings.negeri_polygon_collection]
    negeri_gdf = load_negeri_geodataframe(negeri_coll)

    region_polygons = list(build_region_polygons(negeri_gdf))
    persist_malaysia_polygons(region_polygons)

    count = len(region_polygons)
    logger.info(f"Successfully inserted {count} MalaysiaPolygon documents.")

    return count


if __name__ == "__main__":
    run_malaysia_polygon_pipeline()
