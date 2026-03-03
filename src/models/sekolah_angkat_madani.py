from __future__ import annotations

from datetime import datetime
from typing import ClassVar, Optional

from pydantic import BaseModel, Field, field_validator

from src.config.settings import get_settings
from src.core.time import _utc_now
from src.models.negeri_enum import NegeriEnum

_settings = get_settings()


class SekolahAngkatMadani(BaseModel):
	"""Schema for sekolah angkat madani records."""

	collection_name: ClassVar[str] = _settings.sekolah_angkat_madani_collection

	negeri: NegeriEnum | None = Field(default=None, alias="NEGERI")
	ppd: Optional[str] = Field(default=None, alias="PPD")
	kodSekolah: str = Field(..., alias="KOD SEKOLAH")
	namaSekolah: Optional[str] = Field(default=None, alias="NAMA SEKOLAH")
	createdAt: datetime = Field(default_factory=_utc_now, description="UTC timestamp when the document was created")
	checksum: Optional[str] = Field(default=None, description="SHA-256 hash computed with certain fields excluded")

	@field_validator("ppd", "namaSekolah", mode="before")
	def empty_to_none(cls, value):
		if value is None:
			return None
		text = str(value).strip()
		return None if text == "" else text

	@field_validator("kodSekolah", mode="before")
	def validate_kod_sekolah(cls, value: str | None) -> str:
		text = "" if value is None else str(value).strip()
		if not text:
			raise ValueError("kodSekolah is required")
		return text

	@field_validator("negeri", mode="before")
	def normalize_negeri(cls, value):
		if value is None:
			return None
		if isinstance(value, NegeriEnum):
			return value

		text = str(value).strip().upper()
		if not text:
			return None
		normalized = text.replace(" ", "_")
		try:
			return NegeriEnum(normalized)
		except ValueError as exc:
			raise ValueError(f"Invalid negeri value: {text}") from exc

	model_config = {
		"populate_by_name": True,
		"extra": "ignore",
	}

	def to_document(self) -> dict:
		data = self.model_dump(exclude_none=False)
		data["_id"] = data.get("kodSekolah", self.kodSekolah)
		return data


__all__ = ["SekolahAngkatMadani"]
