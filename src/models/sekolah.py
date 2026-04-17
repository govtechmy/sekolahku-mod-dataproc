from __future__ import annotations

from typing import ClassVar, Optional

from enum import Enum

from datetime import datetime, timezone

from pydantic import BaseModel, Field, field_validator
from src.config.settings import get_settings
from src.models.negeri_enum import NegeriEnum
from src.core.time import _utc_now


MISSING_VALUES = {"TIADA", "", "NONE", "-", "--", "BELUM ADA"}
BOOL_ADA_MAP = {"ADA": True, "TIADA": False, "": None}
BOOL_YA_MAP = {"YA": True, "TIDAK": False, "": None}
PERINGKAT_VALUES = {"RENDAH", "MENENGAH"}

_settings = get_settings()


class SekolahStatus(str, Enum):
    ACTIVE = "ACTIVE"
    INACTIVE = "INACTIVE"


class PeringkatEnum(str, Enum):
    """Enum for school tier (Rendah/Menengah)."""
    RENDAH = "RENDAH"
    MENENGAH = "MENENGAH"


class Sekolah(BaseModel):
    collection_name: ClassVar[str] = _settings.sekolah_collection

    negeri: NegeriEnum | None = Field(default=None, alias="NEGERI")
    ppd: Optional[str] = Field(default=None, alias="PPD")
    parlimen: Optional[str] = Field(default=None, alias="PARLIMEN")
    dun: Optional[str] = Field(default=None, alias="DUN")
    peringkat: PeringkatEnum | None = Field(default=None, alias="PERINGKAT")
    jenisLabel: Optional[str] = Field(default=None, alias="JENIS/LABEL")
    kodSekolah: str = Field(..., alias="KODSEKOLAH")
    namaSekolah: Optional[str] = Field(default=None, alias="NAMASEKOLAH")
    alamatSurat: Optional[str] = Field(default=None, alias="ALAMATSURAT")
    poskodSurat: Optional[int] = Field(default=None, alias="POSKODSURAT")
    bandarSurat: Optional[str] = Field(default=None, alias="BANDARSURAT")

    noTelefon: Optional[str] = Field(default=None, alias="NOTELEFON")
    noFax: Optional[str] = Field(default=None, alias="NOFAX")
    email: Optional[str] = Field(default=None, alias="EMAIL")

    lokasi: Optional[str] = Field(default=None, alias="LOKASI")
    gred: Optional[str] = Field(default=None, alias="GRED")
    bantuan: Optional[str] = Field(default=None, alias="BANTUAN")
    bilSesi: Optional[str] = Field(default=None, alias="BILSESI")
    sesi: Optional[str] = Field(default=None, alias="SESI")

    enrolmenPrasekolah: Optional[int] = Field(default=None, alias="ENROLMEN PRASEKOLAH")
    enrolmen: Optional[int] = Field(default=None, alias="ENROLMEN")
    enrolmenKhas: Optional[int] = Field(default=None, alias="ENROLMEN KHAS")
    guru: Optional[int] = Field(default=None, alias="GURU")

    prasekolah: Optional[bool] = Field(default=None, alias="PRASEKOLAH")
    integrasi: Optional[bool] = Field(default=None, alias="INTEGRASI")

    koordinatXX: Optional[float] = Field(default=None, alias="KOORDINATXX")
    koordinatYY: Optional[float] = Field(default=None, alias="KOORDINATYY")

    skmLEQ150: Optional[bool] = Field(default=None, alias="SKM<=150")

    isSekolahAngkatMADANI: Optional[bool] = Field(default=None, description="Flag indicating Sekolah Angkat MADANI participation")

    status: SekolahStatus | None = Field(default=None, description="Status of the school")
    checksum: Optional[str] = Field(default=None, description="SHA-256 hash computed with certain fields excluded")
    createdAt: datetime = Field(default_factory=_utc_now, description="UTC timestamp when the document was created")

    @field_validator("noTelefon", "noFax", mode="before")
    def empty_to_none(cls, value):
        if value is None:
            return None
        text = str(value).strip().upper()
        return None if text in MISSING_VALUES else value

    @field_validator("kodSekolah", mode="before")
    def validate_kod_sekolah(cls, value: str | None) -> str:
        text = "" if value is None else str(value).strip()
        if not text or text.upper() in MISSING_VALUES:
            raise ValueError("kodSekolah is required")
        return text

    @field_validator("parlimen", mode="before")
    def normalize_parlimen(cls, value):
        if value is None:
            return None
        text = str(value).strip()
        if not text:
            return None
        text = text.upper()
        return text.replace(" ", "_")

    @field_validator("dun", mode="before")
    def normalize_dun(cls, value):
        if value is None:
            return None
        text = str(value).strip()
        return text or None

    @field_validator("peringkat", mode="before")
    def normalize_peringkat(cls, value):
        if value is None:
            return None
        if isinstance(value, PeringkatEnum):
            return value
        text = str(value).strip().upper()
        if not text or text in MISSING_VALUES:
            return None
        if text in {"RENDAH", "MENENGAH"}:
            return PeringkatEnum(text)
        return None

    @field_validator(
        "poskodSurat",
        "enrolmenPrasekolah",
        "enrolmen",
        "enrolmenKhas",
        "guru",
        mode="before",
    )
    def parse_ints(cls, value):
        if value is None or str(value).strip() == "":
            return None
        try:
            return int(str(value).strip())
        except ValueError:
            return None

    @field_validator("prasekolah", "integrasi", mode="before")
    def parse_bool_ada_tiada(cls, value):
        if value is None:
            return None
        if isinstance(value, bool):
            return value
        text = str(value).strip().upper()
        return BOOL_ADA_MAP.get(text, None)

    @field_validator("skmLEQ150", mode="before")
    def parse_bool_ya(cls, value):
        if value is None:
            return None
        if isinstance(value, bool):
            return value
        text = str(value).strip().upper()
        return BOOL_YA_MAP.get(text, None)

    @field_validator("koordinatXX", "koordinatYY", mode="before")
    def parse_float(cls, value):
        if value is None or str(value).strip() == "":
            return None
        try:
            return float(str(value).strip())
        except ValueError:
            return None

    @field_validator("status", mode="before")
    def normalize_status(cls, value):
        if value is None:
            return None
        if isinstance(value, SekolahStatus):
            return value
        text = str(value).strip()
        if not text:
            return None
        try:
            return SekolahStatus(text.upper())
        except ValueError as exc:
            raise ValueError("status must be 'ACTIVE' or 'INACTIVE'") from exc

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
        data["_id"] = data.get("kodSekolah", self.kodSekolah)
        status_value = data.get("status")
        if isinstance(status_value, SekolahStatus):
            data["status"] = status_value.value
        peringkat_value = data.get("peringkat")
        if isinstance(peringkat_value, PeringkatEnum):
            data["peringkat"] = peringkat_value.value
        if self.koordinatXX is not None and self.koordinatYY is not None:
            data["location"] = {
                "type": "Point",
                "coordinates": [self.koordinatXX, self.koordinatYY],
            }
        else:
            data["location"] = None
        return data


__all__ = ["Sekolah", "PeringkatEnum"]
