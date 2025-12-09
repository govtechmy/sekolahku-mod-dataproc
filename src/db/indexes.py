from pymongo import MongoClient
from src.config.settings import get_settings
from pymongo.errors import PyMongoError

import logging
logger = logging.getLogger(__name__)


def add_index(col, keys, name=None, **options):
    """Create index only if an identical one doesn't exist."""
    existing = col.index_information()
    desired_key = list(keys.items())

    for idx_name, idx_info in existing.items():
        if idx_info.get("key") == desired_key:
            logger.info(f"[SKIP] {col.name}.{idx_name} already matches {keys}")
            return

    try:
        result = col.create_index(keys, name=name, **options)
        logger.info(f"[CREATE] {col.name}.{result}")
    except PyMongoError as e:
        logger.warning(f"[ERROR] Skipping index for {col.name}: {e}")


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
    add_index(col, {"data.infoLokasi.location": 1}, name="location")


def main():
    settings = get_settings()
    db = MongoClient(settings.mongo_uri)[settings.db_name]

    create_index_sekolah(db, settings)
    create_index_negeri_parlimen_kod_sekolah(db, settings)
    create_index_entiti_sekolah(db, settings)

    logger.info("Index added successfully.")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()