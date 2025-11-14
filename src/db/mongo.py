"""MongoDB client helpers."""
from __future__ import annotations

from functools import lru_cache

from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.database import Database

from src.config.settings import Settings


@lru_cache
def _client(uri: str) -> MongoClient:
    """Return a cached Mongo client for the given URI."""
    return MongoClient(uri)


def get_database(settings: Settings) -> Database:
    """Return the project database using provided settings."""
    client = _client(settings.mongo_uri)
    return client[settings.db_name]


def get_collection(settings: Settings, name: str) -> Collection:
    """Return a collection by name from the configured database."""
    return get_database(settings)[name]
