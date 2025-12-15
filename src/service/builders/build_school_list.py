import logging

from pymongo.errors import PyMongoError
from botocore.exceptions import ClientError

from src.config.settings import get_settings
from src.core.db import get_mongo_client
from src.core.s3 import upload_json_to_s3
from src.config.settings import Settings


logger = logging.getLogger(__name__)


JSON_FILENAME = "school-list.json"


def build_school_list(docs):
    mapping = []

    for doc in docs:
        key = doc.get("kodSekolah")
        value = doc.get("namaSekolah")

        if key and value:
            mapping.append({"KODSEKOLAH": key, "NAMASEKOLAH": value})

    return mapping


def generate_and_upload_school_list() -> int:
    """Generate school-list.json from MongoDB and upload to S3.

    Returns the number of school entries generated.
    """

    settings = get_settings()

    # Read from MongoDB
    client = get_mongo_client()
    try:
        db = client[settings.db_name]
        collection = db[settings.entiti_sekolah_collection]
        cursor = (collection.find({}, {"_id": 1, "kodSekolah": 1, "namaSekolah": 1}).batch_size(settings.builders_batch_size))

        payload = []
        for batch_doc in cursor:
            # Reuse build_school_list for a single-item list to keep transformation logic consistent
            school_entries = build_school_list([batch_doc])
            if school_entries:
                payload.extend(school_entries)

        logger.info("Generated %d school entries for %s", len(payload), JSON_FILENAME)

    except PyMongoError:
        logger.exception("MongoDB error while generating school list")
        raise
    finally:
        client.close()

    key = f"{settings.s3_prefix_common}/{JSON_FILENAME}"
    logger.info("Uploading %s to bucket=%s prefix=%s", JSON_FILENAME, settings.s3_bucket_public, key)
    upload_json_to_s3(payload, settings.s3_bucket_public, key)

    logger.info("Successfully uploaded %s to S3", JSON_FILENAME)

    return len(payload)