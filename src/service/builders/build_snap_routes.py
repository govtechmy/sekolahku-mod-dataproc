import logging

from pymongo.errors import PyMongoError
from botocore.exceptions import ClientError

from src.config.settings import get_settings
from src.core.db import get_mongo_client
from src.core.s3 import upload_json_to_s3


logger = logging.getLogger(__name__)


FIXED_ROUTES = [
    "/",
    "/home",
    "/about",
    "/carian-sekolah",
    "/siaran",
]

JSON_FILENAME = "snap-routes.json"


def build_snap_routes(docs):
    routes = list(FIXED_ROUTES)
    for doc in docs:
        code = doc.get("KODSEKOLAH") or str(doc.get("_id"))
        routes.append(f"/ms/halaman-sekolah/{code}")
    return routes


def generate_and_upload_snap_routes() -> int:
    """Generate snap-routes.json from MongoDB and upload to S3.

    Returns the number of routes generated.
    """

    settings = get_settings()
    client = get_mongo_client()
    try:
        db = client[settings.db_name]
        collection = db[settings.entiti_sekolah_collection]
        cursor = collection.find({}, {"_id": 1, "KODSEKOLAH": 1}).batch_size(settings.builders_batch_size)

        payload = list(FIXED_ROUTES)
        for doc in cursor:
            code = doc.get("KODSEKOLAH") or str(doc.get("_id"))
            payload.append(f"/halaman-sekolah/{code}")

        logger.info("Generated %d snap routes (including fixed routes)", len(payload))

    except PyMongoError:
        logger.exception("MongoDB error while generating snap routes")
        raise
    finally:
        client.close()
    
    key = f"{settings.s3_prefix_common}/{JSON_FILENAME}"
    logger.info("Uploading %s to bucket=%s prefix=%s", JSON_FILENAME, settings.s3_bucket_public, key)
    upload_json_to_s3(payload, settings.s3_bucket_public, key)

    logger.info("Successfully uploaded %s to S3", JSON_FILENAME)

    return len(payload)