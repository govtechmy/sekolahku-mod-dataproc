from __future__ import annotations

import logging
from typing import Any, Dict, List

from pydantic import ValidationError
from pymongo.collection import Collection

from src.models import Sekolah
from src.models.analitikSekolah import AnalitikSekolah

logger = logging.getLogger(__name__)

ANALITIK_SEKOLAH_COLLECTION = AnalitikSekolah.collection_name


def _build_analitik_document(sekolah_list: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Build analytics document from list of sekolah documents."""
    validated_sekolah = []
    
    for raw in sekolah_list:
        try:
            sekolah = Sekolah.model_validate(raw)
            validated_sekolah.append(sekolah)
        except ValidationError as exc: 
            logger.warning("Skipping sekolah document due to validation error: %s", exc)
            continue
    
    if not validated_sekolah:
        logger.warning("No valid sekolah documents found for analytics")
        return {}
    
    analitik = AnalitikSekolah.from_sekolah_list(validated_sekolah)
    return analitik.to_document()


def compute_analitik_sekolah(collection: Collection) -> List[Dict[str, Any]]:
    """Project sekolah collection into the AnalitikSekolah aggregation view."""
    
    # Fetch all documents from the sekolah collection
    sekolah_docs = list(collection.find({}))
    logger.info("Found %s sekolah documents for analytics processing", len(sekolah_docs))
    
    if not sekolah_docs:
        logger.warning("No sekolah documents found in collection")
        return []
    
    documents: List[Dict[str, Any]] = []
    
    # Create single national analytics document
    national_doc = _build_analitik_document(sekolah_docs)
    if national_doc:
        documents.append(national_doc)
    
    logger.info("Created %s analytics document", len(documents))
    return documents


__all__ = ["compute_analitik_sekolah", "ANALITIK_SEKOLAH_COLLECTION"]