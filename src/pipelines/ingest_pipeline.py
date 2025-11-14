"""High-level ingestion pipeline orchestration."""
from __future__ import annotations

import logging
from typing import Dict, Any

from src.config.settings import Settings
from src.services.ingestion import ingest

logger = logging.getLogger(__name__)


def run(settings: Settings) -> Dict[str, int]:
    """Execute the schools ingestion pipeline."""
    logger.debug("Pipeline run invoked with settings: %s", settings.model_dump(mode="json"))
    result = ingest(settings)
    logger.info("Pipeline completed with result: %s", result)
    return result


def run_with_overrides(**overrides: Any) -> Dict[str, int]:
    """Convenience helper to run pipeline using env settings updated with overrides."""
    from src.config.settings import get_settings

    settings = get_settings().model_copy(update=overrides)
    return run(settings)
