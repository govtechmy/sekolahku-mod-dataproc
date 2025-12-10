from pymongo import MongoClient
from src.config.settings import get_settings
from pymongo.errors import PyMongoError

import logging
logger = logging.getLogger(__name__)


def add_index(col, keys, name=None, **options):
    """
    Checks and fixes the index if needed.
    - If the index exists and matches: skip
    - If the index exists but differs: drop & recreate
    - If no index exists: create it
    """
    existing = col.index_information()

    desired_key = list(keys.items())

    # Check if an index with this name already exists
    if name in existing:
        current = existing[name]
        current_key = current.get("key")

        # If key matches -> skip
        if current_key == desired_key:
            logger.info(f"[SKIP] {col.name}.{name} already matches {keys}")
            return

        # Key mismatch -> drop
        logger.warning(f"[DROP] {col.name}.{name} has wrong key {current_key}, expected {desired_key}")
        col.drop_index(name)

    # Check if an index with same key but different name exists
    for idx_name, idx_info in existing.items():
        if idx_info.get("key") == desired_key:
            logger.info(f"[SKIP] {col.name}.{idx_name} already provides key {keys}")
            return

    result = col.create_index(keys, name=name, **options)
    logger.info(f"[CREATE] {col.name}.{result} created")

def create_index_sekolah(db, settings):
    col = db[settings.sekolah_collection]

    add_index(col, {"kodSekolah": 1}, unique=True, name="kodSekolah_unique")
    add_index(col, {"negeri": 1}, name="negeri")
    add_index(col, {"negeri": 1, "bandarSurat": 1}, name="negeri_bandarSurat")
    add_index(col, {"negeri": 1, "dun": 1}, name="negeri_dun")
    add_index(col, {"negeri": 1, "ppd": 1}, name="negeri_ppd")
    add_index(col, {"location": "2dsphere"}, name="location")


def create_index_negeri_parlimen_kod_sekolah(db, settings):
    col = db[settings.negeri_parlimen_kod_sekolah_collection]

    add_index(col, {"negeri": 1}, name="negeri")
    add_index(col, {"parlimen": 1}, name="parlimen")


def create_index_entiti_sekolah(db, settings):
    col = db[settings.entiti_sekolah_collection]

    add_index(col, {"data.infoPentadbiran.negeri": 1}, name="negeri")
    add_index(col, {"data.infoPentadbiran.parlimen": 1}, name="parlimen")
    add_index(col, {"data.infoLokasi.location": "2dsphere"}, name="location")

def create_index_negeri_polygon(db, settings):
    col = db[settings.negeri_polygon_collection]

    add_index(col, {"negeri": 1}, name="negeri")
    add_index(col, {"geometry": "2dsphere"}, name="geometry")

def create_index_parlimen_polygon(db, settings):
    col = db[settings.parlimen_polygon_collection]

    add_index(col, {"negeri": 1}, name="negeri")
    add_index(col, {"parlimen": 1}, name="parlimen")
    add_index(col, {"geometry": "2dsphere"}, name="geometry")

def main():
    settings = get_settings()
    db = MongoClient(settings.mongo_uri)[settings.db_name]

    create_index_sekolah(db, settings)
    create_index_negeri_parlimen_kod_sekolah(db, settings)
    create_index_entiti_sekolah(db, settings)
    create_index_negeri_polygon(db, settings)
    create_index_parlimen_polygon(db, settings)

    logger.info("Indexes added successfully.")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()