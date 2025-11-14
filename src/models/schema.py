from typing import Optional, ClassVar
from pydantic import BaseModel, Field, EmailStr, field_validator

TIADA_VALUES = {"TIADA", "", "NONE", "-", "--", "BELUM ADA"}
ADA_MAP = {"ADA": True, "TIADA": False, "": None}
BOOL_YA_MAP = {"YA": True, "TIDAK": False, "": None}

class School(BaseModel):
    collection_name: ClassVar[str] = "schools"
    negeri: Optional[str] = Field(default=None, alias="NEGERI")
    ppd: Optional[str] = Field(default=None, alias="PPD")
    parlimen: Optional[str] = Field(default=None, alias="PARLIMEN")
    dun: Optional[str] = Field(default=None, alias="DUN")
    peringkat: Optional[str] = Field(default=None, alias="PERINGKAT")
    jenisLabel: Optional[str] = Field(alias="JENIS/LABEL")
    kodSekolah: Optional[str] = Field(..., alias="KODSEKOLAH", description="Primary code of the school")
    namaSekolah: Optional[str] = Field(default=None, alias="NAMASEKOLAH")
    alamatSurat: Optional[str] = Field(default=None, alias="ALAMATSURAT")
    poskodSurat: Optional[int] = Field(default=None, alias="POSKODSURAT")
    bandarSurat: Optional[str] = Field(default=None, alias="BANDARSURAT")

    noTelefon: Optional[str] = Field(default=None, alias="noTelefon")
    noFax: Optional[str] = Field(default=None, alias="noFax")
    email: Optional[EmailStr] = Field(default=None, alias="EMAIL")

    lokasi: Optional[str] = Field(default=None, alias="LOKASI")
    gred: Optional[str] = Field(default=None, alias="GRED")
    bantuan: Optional[str] = Field(default=None, alias="BANTUAN")
    bilSesi: Optional[str] = Field(default=None, alias="BILSESI")
    sesi: Optional[str] = Field(default=None, alias="SESI")

    enrolmenPrasekolah: Optional[int] = Field(alias="ENROLMEN PRASEKOLAH", default=None)
    enrolmen: Optional[int] = Field(alias="ENROLMEN", default=None)
    enrolmenKhas: Optional[int] = Field(alias="ENROLMEN KHAS", default=None)
    guru: Optional[int] = Field(alias="GURU", default=None)

    prasekolah: Optional[bool] = Field(alias="PRASEKOLAH", default=None)
    integrasi: Optional[bool] = Field(alias="INTEGRASI", default=None)

    koordinatXX: Optional[float] = Field(alias="KOORDINATXX", default=None)
    koordinatYY: Optional[float] = Field(alias="KOORDINATYY", default=None)

    skmLEQ150: Optional[bool] = Field(alias="SKM<=150", default=None)

    @field_validator("noTelefon", "noFax", mode="before")
    def empty_to_none(cls, v):
        if v is None:
            return None
        v_str = str(v).strip().upper()
        return None if v_str in TIADA_VALUES else v

    @field_validator("email", mode="before")
    def normalize_email(cls, v):
        if v is None:
            return None
        v_str = str(v).strip()
        if not v_str:
            return None
        if v_str.upper() in TIADA_VALUES:
            return None
        # Remove trailing punctuation that commonly appears in the source dataset.
        v_str = v_str.rstrip(".")
        while "@@" in v_str:
            v_str = v_str.replace("@@", "@")
        return v_str or None

    @field_validator("parlimen", "dun", mode="before")
    def parse_str(cls, v):
        if v is None:
            return None
        v_str = str(v).strip()
        return v_str or None

    @field_validator("poskodSurat", "enrolmenPrasekolah", "enrolmen", "enrolmenKhas", "guru", mode="before")
    def parse_ints(cls, v):
        if v is None or str(v).strip() == "":
            return None
        try:
            return int(str(v).strip())
        except ValueError:
            return None

    @field_validator("prasekolah", "integrasi", mode="before")
    def parse_ada_tiada(cls, v):
        if v is None:
            return None
        v_up = str(v).strip().upper()
        return ADA_MAP.get(v_up, None)

    @field_validator("skmLEQ150", mode="before")
    def parse_bool_ya(cls, v):
        if v is None:
            return None
        v_up = str(v).strip().upper()
        return BOOL_YA_MAP.get(v_up, None)

    @field_validator("koordinatXX", "koordinatYY", mode="before")
    def parse_float(cls, v):
        if v is None or str(v).strip() == "":
            return None
        try:
            return float(str(v).strip())
        except ValueError:
            return None

    model_config = {
        "populate_by_name": True,
        "extra": "ignore",
    }

    def to_document(self) -> dict:
        doc = self.model_dump(exclude_none=True)
        if self.koordinatXX is not None and self.koordinatYY is not None:
            doc["location"] = {
                "type": "Point",
                "coordinates": [self.koordinatXX, self.koordinatYY],
            }
        return doc
