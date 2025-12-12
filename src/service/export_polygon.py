import json
import logging
from typing import Dict, List, Any

from pymongo import MongoClient

from src.config.settings import get_settings
from src.core.s3 import upload_json_to_s3

logger = logging.getLogger(__name__)


def export_negeri_polygons() -> Dict[str, Any]:
    """
    Export all Negeri polygons from MongoDB to S3.
    
    Returns:
        Summary dictionary with success count and any errors.
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
    
    try:
        # Fetch all negeri polygons
        documents = collection.find({})
        
        for doc in documents:
            negeri = doc.get("negeri")
            
            if not negeri:
                logger.warning("Skipping document without 'negeri' field")
                summary["failed"] += 1
                continue
            
            # Prepare the JSON payload
            polygon_data = {
                "negeri": negeri,
                "geometry": doc.get("geometry"),
                "updatedAt": doc.get("updatedAt").isoformat() if doc.get("updatedAt") else None
            }
            
            try:
                upload_json_to_s3(
                    data=polygon_data,
                    bucket=settings.s3_bucket_name,
                    key=f"polygon/{negeri}/{negeri}.json"
                )
                logger.info(f"✓ Exported Negeri polygon: {negeri} to polygon/{negeri}/{negeri}.json")
                summary["success"] += 1
            except Exception as e:
                error_msg = f"Failed to export {negeri}: {str(e)}"
                logger.error(error_msg)
                summary["failed"] += 1
                summary["errors"].append(error_msg)
        
        logger.info(f"Negeri polygon export completed: {summary['success']} succeeded, {summary['failed']} failed")
        
    except Exception as e:
        logger.exception(f"Error during negeri polygon export: {e}")
        summary["errors"].append(str(e))
    finally:
        client.close()
    
    return summary


def export_parlimen_polygons() -> Dict[str, Any]:
    """
    Export all Parlimen polygons from MongoDB to S3.
    
    Returns:
        Summary dictionary with success count and any errors.
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
    
    try:
        # Fetch all parlimen polygons
        documents = collection.find({})
        
        for doc in documents:
            negeri = doc.get("negeri")
            parlimen = doc.get("parlimen")
            
            if not negeri or not parlimen:
                logger.warning(f"Skipping document without 'negeri' or 'parlimen' field: {doc.get('_id')}")
                summary["failed"] += 1
                continue
            
            # Prepare the JSON payload
            polygon_data = {
                "negeri": negeri,
                "parlimen": parlimen,
                "geometry": doc.get("geometry"),
                "updatedAt": doc.get("updatedAt").isoformat() if doc.get("updatedAt") else None
            }
            
            try:
                upload_json_to_s3(
                    data=polygon_data,
                    bucket=settings.s3_bucket_name,
                    key=f"polygon/{negeri}/{parlimen}.json"
                )
                logger.info(f"✓ Exported Parlimen polygon: {negeri}/{parlimen} to polygon/{negeri}/{parlimen}.json")
                summary["success"] += 1
            except Exception as e:
                error_msg = f"Failed to export {negeri}/{parlimen}: {str(e)}"
                logger.error(error_msg)
                summary["failed"] += 1
                summary["errors"].append(error_msg)
        
        logger.info(f"Parlimen polygon export completed: {summary['success']} succeeded, {summary['failed']} failed")
        
    except Exception as e:
        logger.exception(f"Error during parlimen polygon export: {e}")
        summary["errors"].append(str(e))
    finally:
        client.close()
    
    return summary


def export_all_polygons() -> Dict[str, Any]:
    """
    Ensures both Negeri and Parlimen polygons are exported to S3.
    Provides a combined summary of the export process.
    Used by the API endpoint to trigger polygon export.
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
    
    logger.info(
        f"Polygon export completed: {combined_summary['total_success']} total succeeded, "
        f"{combined_summary['total_failed']} total failed"
    )
    
    return combined_summary
