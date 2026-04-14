from __future__ import annotations

from datetime import datetime
from typing import ClassVar

from pydantic import BaseModel, Field, ConfigDict

from src.config.settings import get_settings
from src.core.time import _utc_now


_settings = get_settings()


class DatasetStatus(BaseModel):
	"""Represents the last refresh timestamp for a dataset ingestion."""

	model_config = ConfigDict(populate_by_name=True)

	collection_name: ClassVar[str] = _settings.dataset_status_collection

	id: str = Field(..., alias="_id", description="Dataset identifier (e.g. sekolah, institusi, analitik)")
	lastUpdatedAt: datetime = Field(default_factory=_utc_now, description="UTC timestamp of last successful ingestion")
	fileVersion: str = Field(..., description="Version of the file name e.g SenaraiSekolahWeb_Mac2026")

	def to_document(self) -> dict:
		return self.model_dump(by_alias=True)
