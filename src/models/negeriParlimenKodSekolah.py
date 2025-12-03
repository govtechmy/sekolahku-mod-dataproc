from __future__ import annotations

from typing import ClassVar, List, Optional

from enum import Enum

from pydantic import BaseModel, Field, field_validator

from src.config.settings import get_settings


_settings = get_settings()


class NegeriEnum(str, Enum):
    JOHOR = "JOHOR"
    KEDAH = "KEDAH"
    KELANTAN = "KELANTAN"
    MELAKA = "MELAKA"
    NEGERI_SEMBILAN = "NEGERI_SEMBILAN"
    PAHANG = "PAHANG"
    PERAK = "PERAK"
    PERLIS = "PERLIS"
    PULAU_PINANG = "PULAU_PINANG"
    SABAH = "SABAH"
    SARAWAK = "SARAWAK"
    SELANGOR = "SELANGOR"
    TERENGGANU = "TERENGGANU"
    WILAYAH_PERSEKUTUAN_KUALA_LUMPUR = "WILAYAH_PERSEKUTUAN_KUALA_LUMPUR"
    WILAYAH_PERSEKUTUAN_LABUAN = "WILAYAH_PERSEKUTUAN_LABUAN"
    WILAYAH_PERSEKUTUAN_PUTRAJAYA = "WILAYAH_PERSEKUTUAN_PUTRAJAYA"


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
        except ValueError:
            return None

    @field_validator("parlimen", mode="before")
    def normalize_parlimen(cls, value: str | None) -> str | None:
        if value is None:
            return "NONE"
        text = str(value).strip()
        if not text:
            return "NONE"
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
        data = self.model_dump(exclude_none=True)

        negeri_value = None
        if "negeri" in data and isinstance(data["negeri"], NegeriEnum):
            negeri_value = data["negeri"].value
            data["negeri"] = negeri_value
        else:
            negeri_value = data.get("negeri")
        parlimen_value = data.get("parlimen") or "NONE"
        if negeri_value is not None and parlimen_value is not None:
            data["_id"] = f"{negeri_value}::{parlimen_value}"
        return data


__all__ = ["NegeriParlimenKodSekolah", "NegeriEnum"]
