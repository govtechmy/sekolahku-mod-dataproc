from __future__ import annotations

import os
import json
import logging
from typing import Optional

import boto3
from dotenv import load_dotenv
from pydantic_settings import BaseSettings, SettingsConfigDict

logger = logging.getLogger(__name__)


load_dotenv()


def _load_from_aws_secrets_manager_if_configured() -> None:
    """If AWS_SECRETS_NAME is set, attempt to load and inject secret values.

    - Secret value is expected to be a JSON object of key/value pairs.
    - If JSON parsing fails, falls back to parsing .env-style lines.
    - On any error, silently falls back to existing environment (.env already loaded).
    """
    secret_name = os.getenv("AWS_SECRETS_NAME")
    if not secret_name:
        return

    try:
        client = boto3.client("secretsmanager")
        response = client.get_secret_value(SecretId=secret_name)
        secret_str = response.get("SecretString")
        
        if not secret_str:
            logger.warning(f"[AWS Secrets] Secret {secret_name} returned empty string")
            return

        try:
            parsed = json.loads(secret_str)
            if isinstance(parsed, dict):
                for key, value in parsed.items():
                    os.environ[key] = str(value)
                return
        except json.JSONDecodeError:
            pass

        # Fallback: parse as .env-style lines (KEY=VALUE)
        for line in secret_str.splitlines():
            stripped = line.strip()
            if not stripped or stripped.startswith("#") or "=" not in stripped:
                continue
            key, value = stripped.split("=", 1)
            os.environ[key.strip()] = value.strip()

    except Exception as e:
        logger.error("Failed to load secrets from AWS Secrets Manager '%s': %s", secret_name, e)
        return

_load_from_aws_secrets_manager_if_configured()


def get_env_str(name: str, default: Optional[str] = None) -> Optional[str]:
    value = os.getenv(name, default)
    return value


def get_env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


class Settings(BaseSettings):

    model_config = SettingsConfigDict(
        env_file=".env",
        case_sensitive=False,
        populate_by_name=True,
        extra="ignore",
    )

    mongo_uri: str = get_env_str("MONGO_URI")
    db_name: str = get_env_str("DB_NAME")
    sekolah_collection: str = get_env_str("SEKOLAH_COLLECTION", "Sekolah")
    entiti_sekolah_collection: str = get_env_str("ENTITI_SEKOLAH_COLLECTION", "EntitiSekolah")
    analitik_sekolah_collection: str = get_env_str("ANALITIK_SEKOLAH_COLLECTION", "AnalitikSekolah")
    negeri_parlimen_kod_sekolah_collection: str = get_env_str("NEGERI_PARLIMEN_KOD_SEKOLAH_COLLECTION", "NegeriParlimenKodSekolah")
    negeri_polygon_collection: str = get_env_str("NEGERI_POLYGON_COLLECTION", "NegeriPolygon")
    parlimen_polygon_collection: str = get_env_str("PARLIMEN_POLYGON_COLLECTION", "ParlimenPolygon")
    csv_path: str = get_env_str("CSV_PATH", "data/sekolah.csv")
    gsheet_id: str = get_env_str("GSHEET_ID")
    gsheet_gid: str = get_env_str("GSHEET_GID")
    gsheet_worksheet_name: str = get_env_str("GSHEET_WORKSHEET_NAME", "Sheet1")
    batch_size: int = get_env_int("BATCH_SIZE", 500)
    port: int = get_env_int("PORT", 8000)
    s3_bucket_name: str = get_env_str("S3_BUCKET_NAME", "my.gov.digital.sekolahku-public-dev")
    entiti_revalidate_batch_size: int = get_env_int("ENTITI_REVALIDATE_BATCH_SIZE", 100)
    entiti_revalidate_max_workers: int = get_env_int("ENTITI_REVALIDATE_MAX_WORKERS", 10)
    entiti_revalidate_temp_prefix: str = get_env_str("ENTITI_REVALIDATE_TEMP_PREFIX", "temp")
    s3_bucket_dataproc: str = get_env_str("S3_BUCKET_DATAPROC")
    s3_prefix_sekolah: str = get_env_str("S3_PREFIX_SEKOLAH")
    s3_prefix_opendosm: str = get_env_str("S3_PREFIX_OPENDOSM", "opendosm/raw")



def get_settings() -> Settings:
    """Return environment settings."""
    return Settings()