"""
Pipeline to load OpenDOSM polygon data into MongoDB seed collections.

Reads extracted GeoJSON data from local files and upserts into:
- negeri_polygon collection
- parliament_polygon collection

Usage:
    python -m src.pipeline.polygons
    python -m src.pipeline.polygons --negeri-only
    python -m src.pipeline.polygons --parliament-only
"""
import os
import json
import argparse
from pathlib import Path
from datetime import datetime
from typing import Dict, List
import logging
from datetime import datetime, timezone

from pymongo import MongoClient, UpdateOne
from src.config.settings import get_settings
from src.models.polygonsSekolah import NegeriPolygon, ParliamentPolygon

logger = logging.getLogger(__name__)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)

class PolygonLoader:
    """Load polygon data from extracted JSON files into MongoDB."""
    
    def __init__(self):
        self.settings = get_settings()
        self.client = MongoClient(self.settings.mongo_uri)
        self.db = self.client[self.settings.db_name]
        
        # Collections
        self.negeri_collection = self.db["negeri_polygon"]
        self.parliament_collection = self.db["parliament_polygon"]
        
        # Data directories
        self.negeri_dir = Path("data_output/extracted_negeri")
        self.parliament_dir = Path("data_output/extracted")
    
    def normalize_state_name(self, name: str) -> str:
        """
        Normalize state name to uppercase with underscores.
        Example: 'Pulau Pinang' -> 'PULAU_PINANG'
        """
        return name.upper().replace(" ", "_")
    
    def parse_parliament_filename(self, filename: str) -> Dict[str, str]:
        """
        Parse parliament filename to extract components.
        
        Format: STATE_CODE_NAME.json
        Example: JOHOR_P.140_SEGAMAT.json
        
        Returns:
            dict with 'state', 'code', 'name'
        """
        # Remove .json extension
        name_part = filename.replace(".json", "")
        
        # Split by underscore
        parts = name_part.split("_")
        
        if len(parts) < 3:
            raise ValueError(f"Invalid parliament filename format: {filename}")
        
        # Find the parliament code (starts with P.)
        code_idx = None
        for i, part in enumerate(parts):
            if part.startswith("P."):
                code_idx = i
                break
        
        if code_idx is None:
            raise ValueError(f"No parliament code found in filename: {filename}")
        
        state = "_".join(parts[:code_idx])
        code = parts[code_idx]
        name = "_".join(parts[code_idx + 1:])
        
        return {
            "state": state,
            "code": code,
            "name": name
        }
    
    def load_negeri_polygons(self) -> Dict[str, int]:
        """
        Load negeri (state) polygons from extracted JSON files.
        
        Returns:
            Summary dict with counts
        """
        logger.info("Loading negeri polygons...")
        
        if not self.negeri_dir.exists():
            raise FileNotFoundError(f"Negeri data directory not found: {self.negeri_dir}")
        
        json_files = list(self.negeri_dir.glob("*.json"))
        logger.info(f"Found {len(json_files)} negeri JSON files")
        
        operations = []
        processed = 0
        errors = []
        
        for json_file in json_files:
            try:
                with open(json_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                
                # Extract from the structure
                params = data.get("params", {})
                geojson = data.get("geojson", {})
                properties = geojson.get("properties", {})
                geometry = geojson.get("geometry", {})
                
                # Get state name from filename (most reliable)
                negeri_normalized = json_file.stem  # e.g., "JOHOR", "PULAU_PINANG"
                
                # Create document with _id as negeri
                doc = {
                    "_id": negeri_normalized,
                    "code_state": properties.get("code_state"),
                    "geometry": geometry,
                    "createdAt": utc_now()
                }
                
                # Validate with Pydantic (without _id validation)
                validation_doc = doc.copy()
                validation_doc.pop("_id")
                NegeriPolygon(**validation_doc)
                
                # Prepare upsert operation
                operations.append(
                    UpdateOne(
                        {"_id": negeri_normalized},
                        {"$set": doc},
                        upsert=True
                    )
                )
                processed += 1
                
            except Exception as e:
                error_msg = f"Error processing {json_file.name}: {str(e)}"
                logger.error(error_msg)
                errors.append({"file": json_file.name, "error": str(e)})
        
        # Execute bulk upsert with ordered=False to continue on errors
        inserted = 0
        updated = 0
        if operations:
            try:
                result = self.negeri_collection.bulk_write(operations, ordered=False)
                inserted = result.upserted_count
                updated = result.modified_count
                logger.info(f"Negeri polygons: {inserted} inserted, {updated} updated")
            except Exception as e:
                # Bulk write can still partially succeed with ordered=False
                logger.error(f"Bulk write error (some documents may have been written): {str(e)}")
                if hasattr(e, 'details') and 'writeErrors' in e.details:
                    for write_error in e.details['writeErrors']:
                        error_doc = write_error.get('op', {}).get('u', {}).get('$set', {}).get('_id', 'unknown')
                        errors.append({
                            "file": f"{error_doc}.json",
                            "error": write_error.get('errmsg', str(write_error))
                        })
                        logger.warning(f"Skipped {error_doc}: {write_error.get('errmsg', '')}")
        
        # Create geospatial index
        self.negeri_collection.create_index([("geometry", "2dsphere")])
        logger.info("Created 2dsphere index on negeri_polygon.geometry")
        
        return {
            "collection": "negeri_polygon",
            "total_files": len(json_files),
            "processed": processed,
            "inserted": inserted,
            "updated": updated,
            "failed": len(errors),
            "errors": errors
        }
    
    def load_parliament_polygons(self) -> Dict[str, int]:
        """
        Load parliament constituency polygons from extracted JSON files.
        
        Returns:
            Summary dict with counts
        """
        logger.info("Loading parliament polygons...")
        
        if not self.parliament_dir.exists():
            raise FileNotFoundError(f"Parliament data directory not found: {self.parliament_dir}")
        
        # Get all JSON files and filter for parliament only (STATE_P.XXX_NAME format)
        # Exclude files starting with "P." and files with "+" (duplicates)
        all_files = list(self.parliament_dir.glob("*.json"))
        json_files = [
            f for f in all_files 
            if "_P." in f.name and not f.name.startswith("P.") and "+" not in f.name
        ]
        logger.info(f"Found {len(json_files)} parliament JSON files (out of {len(all_files)} total files)")
        
        operations = []
        processed = 0
        errors = []
        
        for json_file in json_files:
            try:
                with open(json_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                
                # Extract from the structure
                params = data.get("params", {})
                geojson = data.get("geojson", {})
                properties = geojson.get("properties", {})
                geometry = geojson.get("geometry", {})
                
                # Parse filename for components
                file_parts = self.parse_parliament_filename(json_file.name)
                
                # Get display name from properties
                parlimen_display = properties.get("parlimen", f"{file_parts['code']} {file_parts['name'].replace('_', ' ').title()}")
                parlimen_full = f"{file_parts['code']} {file_parts['name']}"
                
                # Create document with _id as parlimen
                doc = {
                    "_id": parlimen_full,
                    "negeri": file_parts["state"],
                    "parlimen_code": file_parts["code"],
                    "parlimen_name": file_parts["name"],
                    "parlimen_display": parlimen_display,
                    "code_state": properties.get("code_state"),
                    "code_parlimen": properties.get("code_parlimen"),
                    "geometry": geometry,
                    "createdAt": utc_now()
                }
                
                # Validate with Pydantic (without _id validation)
                validation_doc = doc.copy()
                validation_doc.pop("_id")
                ParliamentPolygon(**validation_doc)
                
                # Prepare upsert operation (use parlimen as unique key)
                operations.append(
                    UpdateOne(
                        {"_id": parlimen_full},
                        {"$set": doc},
                        upsert=True
                    )
                )
                processed += 1
                
            except Exception as e:
                error_msg = f"Error processing {json_file.name}: {str(e)}"
                logger.error(error_msg)
                errors.append({"file": json_file.name, "error": str(e)})
        
        # Execute bulk upsert with ordered=False to continue on errors
        inserted = 0
        updated = 0
        if operations:
            try:
                result = self.parliament_collection.bulk_write(operations, ordered=False)
                inserted = result.upserted_count
                updated = result.modified_count
                logger.info(f"Parliament polygons: {inserted} inserted, {updated} updated")
            except Exception as e:
                # Bulk write can still partially succeed with ordered=False
                logger.error(f"Bulk write error (some documents may have been written): {str(e)}")
                if hasattr(e, 'details') and 'writeErrors' in e.details:
                    for write_error in e.details['writeErrors']:
                        error_doc = write_error.get('op', {}).get('u', {}).get('$set', {}).get('_id', 'unknown')
                        errors.append({
                            "file": f"{error_doc}.json",
                            "error": write_error.get('errmsg', str(write_error))
                        })
                        logger.warning(f"Skipped {error_doc}: {write_error.get('errmsg', '')}")
        
        # Create geospatial index
        self.parliament_collection.create_index([("geometry", "2dsphere")])
        # Create index on negeri for joins
        self.parliament_collection.create_index([("negeri", 1)])
        logger.info("Created indexes on parliament_polygon (geometry, negeri)")
        
        return {
            "collection": "parliament_polygon",
            "total_files": len(json_files),
            "processed": processed,
            "inserted": inserted,
            "updated": updated,
            "failed": len(errors),
            "errors": errors
        }


def main():
    """Main entry point for polygon loading pipeline."""
    parser = argparse.ArgumentParser(description="Load OpenDOSM polygon data into MongoDB")
    parser.add_argument("--negeri-only", action="store_true", help="Load only negeri polygons")
    parser.add_argument("--parliament-only", action="store_true", help="Load only parliament polygons")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    
    args = parser.parse_args()
    
    # Configure logging
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    
    loader = PolygonLoader()
    results = {}
    
    try:
        if args.parliament_only:
            results["parliament"] = loader.load_parliament_polygons()
        elif args.negeri_only:
            results["negeri"] = loader.load_negeri_polygons()
        else:
            # Load both
            results["negeri"] = loader.load_negeri_polygons()
            results["parliament"] = loader.load_parliament_polygons()
        
        # Print summary
        print("\n" + "=" * 60)
        print("POLYGON LOADING SUMMARY")
        print("=" * 60)
        for collection_type, summary in results.items():
            print(f"\n{collection_type.upper()}:")
            print(f"  Collection: {summary['collection']}")
            print(f"  Total files: {summary['total_files']}")
            print(f"  Processed: {summary['processed']}")
            print(f"  Inserted: {summary['inserted']}")
            print(f"  Updated: {summary['updated']}")
            print(f"  Failed: {summary['failed']}")
            if summary['errors']:
                print(f"  Errors:")
                for error in summary['errors'][:5]:  # Show first 5 errors
                    print(f"    - {error['file']}: {error['error']}")
        print("=" * 60)
        
    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}", exc_info=True)
        raise


if __name__ == "__main__":
    main()
