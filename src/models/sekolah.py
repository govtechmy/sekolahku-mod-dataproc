from __future__ import annotations

from typing import ClassVar, Optional

from datetime import datetime, timezone

from pydantic import BaseModel, EmailStr, Field, field_validator
from src.config.settings import get_settings


MISSING_VALUES = {"TIADA", "", "NONE", "-", "--", "BELUM ADA"}
BOOL_ADA_MAP = {"ADA": True, "TIADA": False, "": None}
BOOL_YA_MAP = {"YA": True, "TIDAK": False, "": None}


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


_settings = get_settings()


class Sekolah(BaseModel):
    collection_name: ClassVar[str] = _settings.sekolah_collection

    negeri: Optional[str] = Field(default=None, alias="NEGERI")
    ppd: Optional[str] = Field(default=None, alias="PPD")
    parlimen: Optional[str] = Field(default=None, alias="PARLIMEN")
    dun: Optional[str] = Field(default=None, alias="DUN")
    peringkat: Optional[str] = Field(default=None, alias="PERINGKAT")
    jenisLabel: Optional[str] = Field(default=None, alias="JENIS/LABEL")
    kodSekolah: str = Field(..., alias="KODSEKOLAH")
    namaSekolah: Optional[str] = Field(default=None, alias="NAMASEKOLAH")
    alamatSurat: Optional[str] = Field(default=None, alias="ALAMATSURAT")
    poskodSurat: Optional[int] = Field(default=None, alias="POSKODSURAT")
    bandarSurat: Optional[str] = Field(default=None, alias="BANDARSURAT")

    noTelefon: Optional[str] = Field(default=None, alias="NOTELEFON")
    noFax: Optional[str] = Field(default=None, alias="NOFAX")
    email: Optional[EmailStr] = Field(default=None, alias="EMAIL")

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

    active : bool = Field(default=True, description="Indicates whether the school is active",)
    createdAt: datetime = Field(default_factory=_utc_now, description="UTC timestamp when the document was created",)

    updatedAt: datetime = Field(default_factory=_utc_now, description="UTC timestamp when the document was last generated",)

    @field_validator("noTelefon", "noFax", mode="before")
    def empty_to_none(cls, value):
        if value is None:
            return None
        text = str(value).strip().upper()
        return None if text in MISSING_VALUES else value

    @field_validator("email", mode="before")
    def normalize_email(cls, value):
        if value is None:
            return None
        text = str(value).strip()
        if not text or text.upper() in MISSING_VALUES:
            return None
        text = text.rstrip(".")
        while "@@" in text:
            text = text.replace("@@", "@")
        return text or None

    @field_validator("kodSekolah", mode="before")
    def validate_kod_sekolah(cls, value: str | None) -> str:
        text = "" if value is None else str(value).strip()
        if not text or text.upper() in MISSING_VALUES:
            raise ValueError("kodSekolah is required")
        return text

    @field_validator("parlimen", "dun", mode="before")
    def clean_string(cls, value):
        if value is None:
            return None
        text = str(value).strip()
        return text or None

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

    model_config = {
        "populate_by_name": True,
        "extra": "ignore",
    }

    def to_document(self) -> dict:
        data = self.model_dump(exclude_none=False)
        if self.koordinatXX is not None and self.koordinatYY is not None:
            data["location"] = {
                "type": "Point",
                "coordinates": [self.koordinatXX, self.koordinatYY],
            }
        else:
            data["location"] = None
        return data
