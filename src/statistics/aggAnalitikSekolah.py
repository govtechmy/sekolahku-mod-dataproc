from __future__ import annotations

import logging
from typing import Any, Dict, List

from pydantic import ValidationError
from pymongo.collection import Collection

from src.models import Sekolah
from src.models.analitikSekolah import AnalitikSekolah

logger = logging.getLogger(__name__)

ANALITIK_SEKOLAH_COLLECTION = AnalitikSekolah.collection_name


def _build_analitik_document(sekolah_list: List[Dict[str, Any]], region: str = "ALL") -> Dict[str, Any]:
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
    
    analitik = AnalitikSekolah.from_sekolah_list(validated_sekolah, region=region)
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
    
    national_doc = _build_analitik_document(sekolah_docs, region="ALL")
    if national_doc:
        documents.append(national_doc)
    
    # Group by state (negeri) for regional analytics
    states = {}
    for doc in sekolah_docs:
        negeri = doc.get("negeri")  # Use processed field name (lowercase)
        if negeri and str(negeri).strip():  # Only process if negeri has a value
            negeri_normalized = str(negeri).strip().upper()
            if negeri_normalized not in states:
                states[negeri_normalized] = []
            states[negeri_normalized].append(doc)
    
    # Create analytics for each state
    for negeri, state_docs in states.items():
        if len(state_docs) > 0:  # Only create analytics if there are schools
            state_doc = _build_analitik_document(state_docs, region=negeri)
            if state_doc:
                documents.append(state_doc)
    
    logger.info("Created analytics documents for %s regions (including national)", len(documents))
    return documents


__all__ = ["compute_analitik_sekolah", "ANALITIK_SEKOLAH_COLLECTION"]