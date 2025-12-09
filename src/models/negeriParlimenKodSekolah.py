from __future__ import annotations

from typing import ClassVar, List, Optional

from enum import Enum

from pydantic import BaseModel, Field, field_validator

from src.config.settings import get_settings

from src.models.negeriEnum import NegeriEnum

_settings = get_settings()

class NegeriParlimenKodSekolah(BaseModel):
    """Model representing a (negeri, parlimen) pair and its related school codes."""

    collection_name: ClassVar[str] = _settings.negeri_parlimen_kod_sekolah_collection

    negeri: Optional[NegeriEnum] = Field(default=None, description="State name")
    parlimen: Optional[str] = Field(default=None, description="Parliament name")
    kodSekolahList: List[str] = Field(default_factory=list, description="List of school codes under negeri-parlimen pair")

    @field_validator("negeri", mode="before")
    def normalize_negeri(cls, value):
        if value is None:
            return None
        text = str(value).strip()
        if not text:
            return None
        text = "_".join(text.split()).upper()
        try:
            return NegeriEnum(text)
        # If it doesn't match the enum, return None
        except ValueError:
            return None

    @field_validator("parlimen", mode="before")
    def normalize_parlimen(cls, value: str | None) -> str | None:
        if value is None:
            return None
        text = str(value).strip()
        if not text:
            return None
        text = "_".join(text.split())
        return text.upper()

    @field_validator("kodSekolahList", mode="before")
    def normalize_kod_list(cls, value):
        if value is None:
            return []
        if isinstance(value, str):
            codes = [value]
        else:
            codes = list(value)
        cleaned: list[str] = []
        for code in codes:
            if code is None:
                continue
            text = str(code).strip()
            if not text:
                continue
            cleaned.append(text)
        return cleaned

    def to_document(self) -> dict:
        data = self.model_dump(exclude_none=False)

        negeri_value = None
        if isinstance(self.negeri, NegeriEnum):
            negeri_value = self.negeri.value
            data["negeri"] = negeri_value
        else:
            negeri_value = data.get("negeri")
        # parlimen stays NULL if None
        parlimen_value = self.parlimen
        parlimen_id = parlimen_value if parlimen_value else "UNKNOWN"
        if negeri_value is not None:
            data["_id"] = f"{negeri_value}::{parlimen_id}"

        return data


__all__ = ["NegeriParlimenKodSekolah", "NegeriEnum"]
