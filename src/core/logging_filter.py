import logging
from typing import Iterable

from src.config.settings import Settings

DEFAULT_NOISY_LOGGERS: Iterable[str] = (
    "uvicorn",
    "uvicorn.error",
    "uvicorn.access",
    "pymongo",
    "pymongo.topology",
    "botocore",
    "boto3",
)

def configure_logging(settings: Settings, noisy_loggers: Iterable[str] = DEFAULT_NOISY_LOGGERS) -> None:
    logging.basicConfig(
        level=getattr(logging, settings.log_level.upper(), logging.INFO),
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )

    for name in noisy_loggers:
        logging.getLogger(name).setLevel(logging.WARNING)

