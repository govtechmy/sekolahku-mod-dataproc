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
        case_sensitive=True,
        populate_by_name=True,
        extra="ignore",
    )
    
    log_level: str = get_env_str("LOG_LEVEL", "INFO")
    mongo_uri: str = get_env_str("MONGO_URI")
    db_name: str = get_env_str("DB_NAME")
    sekolah_collection: str = get_env_str("SEKOLAH_COLLECTION", "Sekolah")
    institusi_collection: str = get_env_str("INSTITUSI_COLLECTION", "Institusi")
    sekolah_angkat_madani_collection: str = get_env_str("SEKOLAH_ANGKAT_MADANI_COLLECTION", "SekolahAngkatMadani")
    entiti_sekolah_collection: str = get_env_str("ENTITI_SEKOLAH_COLLECTION", "EntitiSekolah")
    analitik_sekolah_collection: str = get_env_str("ANALITIK_SEKOLAH_COLLECTION", "AnalitikSekolah")
    asset_sekolah_collection: str = get_env_str("ASSET_SEKOLAH_COLLECTION", "AssetSekolah")
    dataset_status_collection: str = get_env_str("DATASET_STATUS_COLLECTION", "DatasetStatus")
    negeri_parlimen_kod_sekolah_collection: str = get_env_str("NEGERI_PARLIMEN_KOD_SEKOLAH_COLLECTION", "NegeriParlimenKodSekolah")
    negeri_polygon_collection: str = get_env_str("NEGERI_POLYGON_COLLECTION", "NegeriPolygon")
    parlimen_polygon_collection: str = get_env_str("PARLIMEN_POLYGON_COLLECTION", "ParlimenPolygon")
    malaysia_polygon_collection: str = get_env_str("MALAYSIA_POLYGON_COLLECTION", "MalaysiaPolygon")
    logo_sekolah_collection: str = get_env_str("LOGO_SEKOLAH_COLLECTION", "LogoSekolah")
    csv_path: str = get_env_str("CSV_PATH", "data/sekolah.csv")
    gsheet_id: str = get_env_str("GSHEET_ID")
    gsheet_gid: str = get_env_str("GSHEET_GID")
    institusi_gsheet_id: str = get_env_str("GSHEET_ID")
    institusi_gsheet_gid: str = get_env_str("INSTITUSI_GSHEET_GID")
    batch_size: int = get_env_int("BATCH_SIZE", 500)
    port: int = get_env_int("PORT", 8000)
    s3_bucket_public: str = get_env_str("S3_BUCKET_PUBLIC", "my.gov.digital.sekolahku-public-dev")
    s3_bucket_dataproc: str = get_env_str("S3_BUCKET_DATAPROC")
    entiti_revalidate_batch_size: int = get_env_int("ENTITI_REVALIDATE_BATCH_SIZE", 100)
    entiti_revalidate_max_workers: int = get_env_int("ENTITI_REVALIDATE_MAX_WORKERS", 10)
    entiti_revalidate_temp_prefix: str = get_env_str("ENTITI_REVALIDATE_TEMP_PREFIX", "temp")
    builders_batch_size: int = get_env_int("BUILDERS_BATCH_SIZE", 100)
    polygon_export_batch_size: int = get_env_int("POLYGON_EXPORT_BATCH_SIZE", 100)
    asset_export_batch_size: int = get_env_int("ASSET_EXPORT_BATCH_SIZE", 100)
    asset_logo_csv_filename: str = get_env_str("ASSET_LOGO_CSV_FILENAME", "tbi_institusi_induk.csv")
    asset_logo_csv_batch_size: int = get_env_int("ASSET_LOGO_CSV_BATCH_SIZE", 500)
    export_centroid_max_workers: int = get_env_int("EXPORT_CENTROID_MAX_WORKERS", 4)
    sekolah_angkat_madani_filename: str = get_env_str("SEKOLAH_ANGKAT_MADANI_FILENAME", "SENARAI SEKOLAH ANGKAT MADANI.xlsx")

    # Constants
    s3_prefix_sekolah: str = "sekolah/raw"
    s3_prefix_institusi: str = "institusi/raw"
    s3_prefix_opendosm: str = "opendosm/raw"
    s3_prefix_common: str = "common"
    s3_prefix_polygon: str = "polygon"
    s3_prefix_assets: str = "assets/raw"
    s3_prefix_sekolah_angkat_madani: str = "sekolah_angkat_madani/raw"


def get_settings() -> Settings:
    """Return environment settings."""
    try:
        return Settings()
    except Exception as e:
        logger.error("Configuration error while loading Settings: %s", e)
        raise RuntimeError(
            f"Invalid or missing configuration: {e}. Check your .env or environment variables."
        ) from e