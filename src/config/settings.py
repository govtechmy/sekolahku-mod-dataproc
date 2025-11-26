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
    source: str = Field(default="csv", alias="SOURCE")
    csv_path: str = Field(default="data/sekolah.csv", alias="CSV_PATH")
    gsheet_id: str | None = Field(default=None, alias="GSHEET_ID")
    gsheet_worksheet_name: str = Field(default="Sheet1", alias="GSHEET_WORKSHEET_NAME")
    google_credentials_path: str = Field(default="service_account.json", alias="GOOGLE_APPLICATION_CREDENTIALS")
    batch_size: int = Field(default=500, alias="BATCH_SIZE")
    dry_run_flag: int = Field(default=0, alias="DRY_RUN")

    @property
    def dry_run(self) -> bool:
        """Expose dry-run flag as bool for readability."""
        return bool(self.dry_run_flag)


def get_settings() -> Settings:
    """Return environment settings."""
    return Settings()  # type: ignore[arg-type]
