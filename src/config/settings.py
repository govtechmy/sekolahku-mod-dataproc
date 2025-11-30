from __future__ import annotations


from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):

    model_config = SettingsConfigDict(
        env_file=".env",
        case_sensitive=False,
        populate_by_name=True,
        extra="ignore",
    )

    mongo_uri: str = Field(default="mongodb://localhost:27017", alias="MONGO_URI")
    db_name: str = Field(default="sekolahku", alias="DB_NAME")
    sekolah_collection: str = Field(default="Sekolah", alias="SEKOLAH_COLLECTION")
    entiti_sekolah_collection: str = Field(default="EntitiSekolah", alias="ENTITI_SEKOLAH_COLLECTION")
    analitik_sekolah_collection: str = Field(default="AnalitikSekolah", alias="ANALITIK_SEKOLAH_COLLECTION")
    csv_path: str = Field(default="data/sekolah.csv", alias="CSV_PATH")
    batch_size: int = Field(default=500, alias="BATCH_SIZE")


def get_settings() -> Settings:
    """Return environment settings."""
    return Settings()  # type: ignore[arg-type]
