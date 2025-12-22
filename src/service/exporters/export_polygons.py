import json
import logging
from typing import Dict, Any
from concurrent.futures import ThreadPoolExecutor

from pymongo import MongoClient

from src.config.settings import get_settings
from src.core.s3 import upload_json_to_s3

logger = logging.getLogger(__name__)


def export_negeri_polygons() -> Dict[str, Any]:
    """
    Export all Negeri polygons from MongoDB to S3.
    """
    settings = get_settings()
    client = MongoClient(settings.mongo_uri)
    db = client[settings.db_name]
    collection = db[settings.negeri_polygon_collection]
    
    summary = {
        "type": "negeri",
        "success": 0,
        "failed": 0,
        "errors": []
    }
    
    logger.info("Starting Negeri polygon export")

    try:
        # Fetch all negeri polygons with batch processing
        cursor = collection.find({}).batch_size(settings.polygon_export_batch_size)
        
        # Use thread pool for parallel S3 uploads (documents are independent)
        with ThreadPoolExecutor(max_workers=settings.entiti_revalidate_max_workers) as executor:
            futures = []

            for doc in cursor:
                negeri = doc.get("negeri")

                if not negeri:
                    logger.warning("Skipping document without 'negeri' field")
                    summary["failed"] += 1
                    continue

                polygon_data = {
                    "negeri": negeri,
                    "geometry": doc.get("geometry"),
                    "updatedAt": doc.get("updatedAt").isoformat() if doc.get("updatedAt") else None
                }

                key = f"{settings.s3_prefix_polygon}/{negeri}/{negeri}.json"

                future = executor.submit(
                    upload_json_to_s3,
                    payload=polygon_data,
                    bucket=settings.s3_bucket_public,
                    key=key
                )
                futures.append((future, negeri, key))

            for future, negeri, key in futures:
                try:
                    future.result()
                    logger.debug(f"Exported Negeri polygon: {negeri} to {key}")
                    summary["success"] += 1
                except Exception as e:
                    error_msg = f"Failed to export {negeri}: {str(e)}"
                    logger.error(error_msg)
                    summary["failed"] += 1
                    summary["errors"].append(error_msg)

        logger.info("Negeri polygon export completed: %d succeeded, %d failed", summary["success"], summary["failed"],)
        
    except Exception as e:
        logger.exception(f"Error during negeri polygon export: {e}")
        summary["errors"].append(str(e))
    finally:
        client.close()
    
    return summary


def export_parlimen_polygons() -> Dict[str, Any]:
    """
    Export all Parlimen polygons from MongoDB to S3.
    """
    settings = get_settings()
    client = MongoClient(settings.mongo_uri)
    db = client[settings.db_name]
    collection = db[settings.parlimen_polygon_collection]
    
    summary = {
        "type": "parlimen",
        "success": 0,
        "failed": 0,
        "errors": []
    }
    
    logger.info("Starting Parlimen polygon export")

    try:
         # Fetch all parlimen polygons with batch processing
        cursor = collection.find({}).batch_size(settings.polygon_export_batch_size)
        
        # Use thread pool for parallel S3 uploads (documents are independent)
        with ThreadPoolExecutor(max_workers=settings.entiti_revalidate_max_workers) as executor:
            futures = []

            for doc in cursor:
                negeri = doc.get("negeri")
                parlimen = doc.get("parlimen")

                if not negeri or not parlimen:
                    logger.warning(
                        "Skipping document without 'negeri' or 'parlimen' field: %s",
                        doc.get("_id"),
                    )
                    summary["failed"] += 1
                    continue

                polygon_data = {
                    "negeri": negeri,
                    "parlimen": parlimen,
                    "geometry": doc.get("geometry"),
                    "updatedAt": doc.get("updatedAt").isoformat() if doc.get("updatedAt") else None
                }

                key = f"{settings.s3_prefix_polygon}/{negeri}/{parlimen}.json"

                future = executor.submit(
                    upload_json_to_s3,
                    payload=polygon_data,
                    bucket=settings.s3_bucket_public,
                    key=key
                )
                futures.append((future, negeri, parlimen, key))

            for future, negeri, parlimen, key in futures:
                try:
                    future.result()
                    logger.debug("Exported Parlimen polygon: %s/%s to %s", negeri, parlimen, key)
                    summary["success"] += 1
                except Exception as e:
                    error_msg = f"Failed to export {negeri}/{parlimen}: {str(e)}"
                    logger.error(error_msg)
                    summary["failed"] += 1
                    summary["errors"].append(error_msg)

        logger.info("Parlimen polygon export completed: %d succeeded, %d failed", summary["success"], summary["failed"],)
        
    except Exception as e:
        logger.exception(f"Error during parlimen polygon export: {e}")
        summary["errors"].append(str(e))
    finally:
        client.close()
    
    return summary


def export_all_polygons() -> Dict[str, Any]:
    """
    Export both Negeri and Parlimen polygons to S3.
    """
    logger.info("Starting polygon export to S3")
    
    negeri_summary = export_negeri_polygons()
    parlimen_summary = export_parlimen_polygons()
    
    combined_summary = {
        "negeri": negeri_summary,
        "parlimen": parlimen_summary,
        "total_success": negeri_summary["success"] + parlimen_summary["success"],
        "total_failed": negeri_summary["failed"] + parlimen_summary["failed"]
    }
    
    logger.info("Polygon export completed: %d total succeeded, %d total failed", combined_summary["total_success"], combined_summary["total_failed"])
    
    return combined_summary
