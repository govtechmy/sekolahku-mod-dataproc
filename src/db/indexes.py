from pymongo import MongoClient
from src.config.settings import get_settings

import logging
logger = logging.getLogger(__name__)

def add_index_sekolah(col):
    """Add indexes to the Sekolah collection."""
    col.create_index({"kodSekolah": 1}, unique=True, name="kodSekolah_unique")
    col.create_index({"negeri": 1}, name="negeri")
    col.create_index({"negeri": 1, "bandarSurat": 1}, name="negeri_bandarSurat")
    col.create_index({"negeri": 1, "dun": 1}, name="negeri_dun")
    col.create_index({"negeri": 1, "ppd": 1}, name="negeri_ppd")
    col.create_index({"location": "2dsphere"}, name="location")


def add_index_negeri_parlimen_kod_sekolah(col):
    """Add indexes to the NegeriParlimenKodSekolah collection."""
    col.create_index({"negeri": 1}, name="negeri")
    col.create_index({"parlimen": 1}, name="parlimen")


def add_index_entiti_sekolah(col):
    """Add indexes to the EntitiSekolah collection."""
    col.create_index({"data.infoPentadbiran.negeri": 1}, name="negeri")
    col.create_index({"data.infoPentadbiran.parlimen": 1}, name="parlimen")
    col.create_index({"data.infoLokasi.location": 1}, name="location")


def main():
    settings = get_settings()
    db = MongoClient(settings.mongo_uri)[settings.db_name]

    add_index_sekolah(db[settings.sekolah_collection])
    add_index_negeri_parlimen_kod_sekolah(db[settings.negeri_parlimen_kod_sekolah_collection])
    add_index_entiti_sekolah(db[settings.entiti_sekolah_collection])

    logger.info("Indexes added successfully.")


if __name__ == "__main__":
    main()
