from __future__ import annotations

import re
from typing import ClassVar, Optional
from datetime import datetime

from pydantic import BaseModel, Field, field_validator
from src.config.settings import get_settings
from src.models.negeri_enum import NegeriEnum
from src.core.time import _utc_now

MISSING_VALUES = {"TIADA", "", "NONE", "-", "--", "BELUM ADA"}

_settings = get_settings()


class Institusi(BaseModel):
    """Schema for programme-based institutions (e.g., IPG)."""

    collection_name: ClassVar[str] = _settings.institusi_collection

    negeri: NegeriEnum | None = Field(default=None, alias="NEGERI")
    ppd: Optional[str] = Field(default=None, alias="PPD")
    jenisLabel: Optional[str] = Field(default=None, alias="JENIS/LABEL")
    kodInstitusi: str = Field(..., alias="KODINSTITUSI")
    namaSekolah: Optional[str] = Field(default=None, alias="NAMASEKOLAH")
    enrolmenPrasekolah: Optional[int] = Field(default=None, alias="ENROLMEN PRA")
    guru: Optional[int] = Field(default=None, alias="GURU")
    status: str | None = Field(default=None, description="Status of the institution (ACTIVE/INACTIVE)")
    createdAt: datetime = Field(default_factory=_utc_now, description="UTC timestamp when the document was created")

    @field_validator("ppd", "jenisLabel", "namaSekolah", mode="before")
    def empty_to_none(cls, value):
        if value is None:
            return None
        text = str(value).strip()
        return None if text.upper() in MISSING_VALUES or text == "" else text

    @field_validator("kodInstitusi", mode="before")
    def validate_kod_institusi(cls, value: str | None) -> str:
        text = "" if value is None else str(value).strip()
        if not text or text.upper() in MISSING_VALUES:
            raise ValueError("kodInstitusi is required")
        return text

    @field_validator("enrolmenPrasekolah", "guru", mode="before")
    def parse_ints(cls, value):
        if value is None:
            return None
        text = str(value).strip()
        if text == "":
            return None

        match = re.match(r"^-?\d+", text)
        if match:
            try:
                return int(match.group())
            except ValueError:
                return None
        return None

    @field_validator("negeri", mode="before")
    def normalize_negeri(cls, value):
        if value is None:
            return None
        if isinstance(value, NegeriEnum):
            return value

        text = str(value).strip().upper()
        if not text or text in MISSING_VALUES:
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
        data["_id"] = data.get("kodInstitusi", self.kodInstitusi)
        return data


__all__ = ["Institusi"]
