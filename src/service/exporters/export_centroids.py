import logging
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict

from pymongo import MongoClient

from src.config.settings import get_settings
from src.core.s3 import upload_json_to_s3


logger = logging.getLogger(__name__)


def export_negeri_centroids() -> Dict[str, Any]:
    settings = get_settings()
    client = MongoClient(settings.mongo_uri)
    db = client[settings.db_name]
    collection = db[settings.negeri_polygon_collection]

    summary: Dict[str, Any] = {"type": "negeri", "success": 0, "failed": 0, "skipped": 0, "errors": [], "keys": []}

    try:
        cursor = collection.find({}, {"_id": 0, "negeri": 1, "centroid": 1}).batch_size(settings.polygon_export_batch_size)

        with ThreadPoolExecutor(max_workers=settings.entiti_revalidate_max_workers) as executor:
            futures = []

            for doc in cursor:
                negeri = doc.get("negeri")
                centroid = doc.get("centroid")

                if not negeri or not centroid:
                    summary["skipped"] += 1
                    continue

                key = f"centroid/NEGERI/{negeri}.json"
                payload = {"negeri": negeri, "centroid": centroid}

                futures.append((executor.submit(upload_json_to_s3, payload, settings.s3_bucket_public, key), negeri, key))

            for future, negeri, key in futures:
                try:
                    future.result()
                    summary["success"] += 1
                    summary["keys"].append(key)
                except Exception as e:
                    logger.exception("Failed exporting centroid for negeri=%s to key=%s", negeri, key)
                    summary["failed"] += 1
                    summary["errors"].append(f"{negeri}: {e}")

        return summary
    except Exception:
        logger.exception("Error during negeri centroid export")
        raise
    finally:
        client.close()


def export_parlimen_centroids() -> Dict[str, Any]:
    settings = get_settings()
    client = MongoClient(settings.mongo_uri)
    db = client[settings.db_name]
    collection = db[settings.parlimen_polygon_collection]

    summary: Dict[str, Any] = {"type": "parlimen", "success": 0, "failed": 0, "skipped": 0, "errors": [], "keys": []}

    try:
        cursor = collection.find({}, {"_id": 0, "parlimen": 1, "centroid": 1}).batch_size(settings.polygon_export_batch_size)

        with ThreadPoolExecutor(max_workers=settings.entiti_revalidate_max_workers) as executor:
            futures = []

            for doc in cursor:
                parlimen = doc.get("parlimen")
                centroid = doc.get("centroid")

                if not parlimen or not centroid:
                    summary["skipped"] += 1
                    continue

                key = f"centroid/PARLIMEN/{parlimen}.json"
                payload = {"parlimen": parlimen, "centroid": centroid}

                futures.append((executor.submit(upload_json_to_s3, payload, settings.s3_bucket_public, key), parlimen, key))

            for future, parlimen, key in futures:
                try:
                    future.result()
                    summary["success"] += 1
                    summary["keys"].append(key)
                except Exception as e:
                    logger.exception("Failed exporting centroid for parlimen=%s to key=%s", parlimen, key)
                    summary["failed"] += 1
                    summary["errors"].append(f"{parlimen}: {e}")

        return summary
    except Exception:
        logger.exception("Error during parlimen centroid export")
        raise
    finally:
        client.close()


def export_malaysia_centroids() -> Dict[str, Any]:
    settings = get_settings()
    client = MongoClient(settings.mongo_uri)
    db = client[settings.db_name]
    collection = db[settings.malaysia_polygon_collection]

    summary: Dict[str, Any] = {"type": "malaysia", "success": 0, "failed": 0, "skipped": 0, "errors": [], "keys": []}

    try:
        cursor = collection.find({}, {"_id": 0, "region": 1, "centroid": 1})

        with ThreadPoolExecutor(max_workers=settings.entiti_revalidate_max_workers) as executor:
            futures = []

            for doc in cursor:
                region = doc.get("region")
                centroid = doc.get("centroid")

                if not region or not centroid:
                    summary["skipped"] += 1
                    continue

                key = f"centroid/MALAYSIA/{region}.json"
                payload = {"region": region, "centroid": centroid}

                futures.append((executor.submit(upload_json_to_s3, payload, settings.s3_bucket_public, key), region, key))

            for future, region, key in futures:
                try:
                    future.result()
                    summary["success"] += 1
                    summary["keys"].append(key)
                except Exception as e:
                    logger.exception("Failed exporting centroid for region=%s to key=%s", region, key)
                    summary["failed"] += 1
                    summary["errors"].append(f"{region}: {e}")

        return summary
    except Exception:
        logger.exception("Error during malaysia centroid export")
        raise
    finally:
        client.close()


def export_all_centroids() -> Dict[str, Any]:
    logger.info("Starting centroid export for negeri, parlimen, and malaysia")

    settings = get_settings()

    with ThreadPoolExecutor(max_workers=settings.entiti_revalidate_max_workers) as executor:
        future_negeri = executor.submit(export_negeri_centroids)
        future_parlimen = executor.submit(export_parlimen_centroids)
        future_malaysia = executor.submit(export_malaysia_centroids)

        negeri_summary = future_negeri.result()
        parlimen_summary = future_parlimen.result()
        malaysia_summary = future_malaysia.result()

    def strip_centroid_prefix(key: str) -> str:
        prefix = "centroid/"
        return key[len(prefix):] if key.startswith(prefix) else key

    manifest = {
        "negeri": [strip_centroid_prefix(k) for k in negeri_summary.get("keys", [])],
        "parlimen": [strip_centroid_prefix(k) for k in parlimen_summary.get("keys", [])],
        "malaysia": [strip_centroid_prefix(k) for k in malaysia_summary.get("keys", [])],
    }

    manifest_key = "centroid/index.json"

    upload_json_to_s3(
        payload=manifest,
        bucket=settings.s3_bucket_public,
        key=manifest_key,
    )

    return {
        "negeri": negeri_summary,
        "parlimen": parlimen_summary,
        "malaysia": malaysia_summary,
        "manifest_key": manifest_key,
    }

