from __future__ import annotations

import asyncio
import logging
from typing import Any, Callable

logger = logging.getLogger(__name__)


async def _safe_to_thread(label: str, func: Callable, *args) -> Any:
    """Run a blocking function in a thread and log failures."""
    try:
        return await asyncio.to_thread(func, *args)
    except Exception:  # noqa: BLE001
        logger.exception("%s failed during startup backfill", label)
        return None


async def run_startup_backfill(
    *,
    missing: set[str],
    settings,
    schedule_scrape_opendosm_polygons_job: Callable,
    core_ingest: Callable,
    run_post_ingestion_pipeline: Callable,
    load_opendosm_negeri_main: Callable,
    load_opendosm_parlimen_main: Callable,
    export_all_polygons: Callable,
    export_all_centroids: Callable,
    generate_and_upload_snap_routes: Callable,
    generate_and_upload_school_list: Callable,
    process_csv_assets: Callable,
) -> None:
    """
    Trigger only the routines required for missing S3 artifacts.

    All callable dependencies are passed in to avoid circular imports with api.py.
    """
    polygon_raw_missing = bool({"raw_opendosm_negeri", "raw_opendosm_parlimen"} & missing)
    polygon_exports_missing = "polygon_exports" in missing
    centroid_missing = "centroid_manifest" in missing

    common_missing = {"common_snap_routes", "common_school_list"} & missing
    assets_manifest_missing = "assets_manifest" in missing
    assets_csv_missing = "assets_csv" in missing
    needs_full_post_pipeline = bool(
        polygon_raw_missing or polygon_exports_missing or centroid_missing
    )
    needs_core_ingestion = bool(common_missing or assets_manifest_missing or needs_full_post_pipeline)

    if polygon_raw_missing:
        logger.info("Backfill: scraping OpenDOSM polygons")
        try:
            await schedule_scrape_opendosm_polygons_job()
        except Exception:  # noqa: BLE001
            logger.exception("Backfill: scraping OpenDOSM polygons failed")

    if polygon_raw_missing or polygon_exports_missing:
        logger.info("Backfill: loading and exporting polygons")
        await _safe_to_thread("Load Negeri polygons", load_opendosm_negeri_main)
        await _safe_to_thread("Load Parlimen polygons", load_opendosm_parlimen_main)
        await _safe_to_thread("Export polygons", export_all_polygons)

    if polygon_raw_missing or polygon_exports_missing or centroid_missing:
        logger.info("Backfill: exporting centroids")
        await _safe_to_thread("Export centroids", export_all_centroids)

    if needs_core_ingestion:
        logger.info("Backfill: running core ingestion before S3 uploads")
        await _safe_to_thread("Run ingestion pipeline", core_ingest, settings)

    if needs_full_post_pipeline:
        logger.info("Backfill: running post-ingestion pipeline for polygons and aggregates")
        await _safe_to_thread("Run post-ingestion pipeline", run_post_ingestion_pipeline, settings)

    if common_missing:
        logger.info("Backfill: regenerating common exports")
        tasks = []
        if "common_snap_routes" in common_missing:
            tasks.append(_safe_to_thread("Generate snap routes", generate_and_upload_snap_routes))
        if "common_school_list" in common_missing:
            tasks.append(_safe_to_thread("Generate school list", generate_and_upload_school_list))
        if tasks:
            await asyncio.gather(*tasks)

    if assets_manifest_missing:
        if assets_csv_missing:
            logger.warning("Skipping asset logo processing because CSV is missing in S3")
        else:
            logger.info("Backfill: processing asset logos")
            await _safe_to_thread("Process asset logos", process_csv_assets, settings)
