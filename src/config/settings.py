from __future__ import annotations

import os
import json
from typing import Optional

import boto3
from dotenv import load_dotenv
from pydantic_settings import BaseSettings, SettingsConfigDict


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
    csv_path: str = get_env_str("CSV_PATH", "data/sekolah.csv")
    batch_size: int = get_env_int("BATCH_SIZE", 500)


def get_settings() -> Settings:
    """Return environment settings."""
    return Settings()  