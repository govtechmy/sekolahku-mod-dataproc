from __future__ import annotations

from typing import ClassVar, Optional, TYPE_CHECKING

from datetime import datetime, timezone
from src.core.time import _utc_now

from pydantic import BaseModel, Field, field_validator
from pydantic import ConfigDict
from typing_extensions import Literal
from src.config.settings import get_settings
from src.models.sekolah import SekolahStatus
from src.models.negeri_enum import NegeriEnum


if TYPE_CHECKING:  # pragma: no cover - imported for type checking only
    from src.models.sekolah import Sekolah


class InfoSekolah(BaseModel):
    jenisLabel: Optional[str] = Field(default=None, description="Type of school label e.g SK, SMK, SMKA, etc.")
    jumlahPelajar: Optional[int] = Field(default=0, description="Total students (enrolmenPrasekolah + enrolmen + enrolmenKhas)")
    jumlahGuru: Optional[int] = Field(default=0, description="Total number of teachers")
    jumlahPelajarEnrolmenKhas: Optional[int] = Field(default=0, description="Number of students in enrolmenKhas category")
    jumlahPelajarTanpaEnrolmenKhas: Optional[int] = Field(default=0, description="Number of students excluding enrolmenKhas category")

class InfoKomunikasi(BaseModel):
    noTelefon: Optional[str] = Field(default=None, description="Primary contact number")
    noFax: Optional[str] = Field(default=None, description="Fax number")
    email: Optional[str] = Field(default=None, description="General contact email")
    alamatSurat: Optional[str] = Field(default=None, description="Mailing address")
    poskodSurat: Optional[str] = Field(default=None, description="Mailing postcode")
    bandarSurat: Optional[str] = Field(default=None, description="Mailing city")

class InfoPentadbiran(BaseModel):
    negeri: NegeriEnum | None = Field(default=None, description="State the school is located in")
    ppd: Optional[str] = Field(default=None, description="Pejabat Pendidikan Daerah (district office)")
    parlimen: Optional[str] = Field(default=None, description="Parliament constituency")
    bantuan: Optional[str] = Field(default=None, description="Bantuan classification")
    bilSesi: Optional[str] = Field(default=None, description="Number of school sessions")
    sesi: Optional[str] = Field(default=None, description="School session")
    prasekolah: Optional[bool] = Field(default=None, description="Has preschool programme")
    integrasi: Optional[bool] = Field(default=None, description="Runs integration programme")

class GeoJSONPoint(BaseModel):
    type: Literal["Point"] = Field(default="Point", description="GeoJSON geometry type")
    coordinates: tuple[float, float] = Field(..., description="(longitude, latitude) coordinate pair")


class InfoLokasi(BaseModel):
    koordinatXX: Optional[float] = Field(default=None, description="Longitude value")
    koordinatYY: Optional[float] = Field(default=None, description="Latitude value")
    location: Optional[GeoJSONPoint] = Field(default=None, description="GeoJSON point for geospatial queries")


class EntitiSekolahData(BaseModel):
    infoSekolah: InfoSekolah
    infoKomunikasi: InfoKomunikasi
    infoPentadbiran: InfoPentadbiran
    infoLokasi: InfoLokasi

_settings = get_settings()


class EntitiSekolah(BaseModel):
    """Aggregated entity view for a school document."""

    collection_name: ClassVar[str] = _settings.entiti_sekolah_collection

    namaSekolah: Optional[str] = Field(default=None, description="Name of the school")
    kodSekolah: str = Field(..., description="Unique school code identifier")
    status: SekolahStatus | None = Field(default=None, description="Status of the school")
    isSekolahAngkatMADANI: Optional[bool] = Field(default=None, description="Flag indicating Sekolah Angkat MADANI participation")
    data: EntitiSekolahData
    createdAt: datetime = Field(default_factory=_utc_now, description="UTC timestamp when the document was created")

    model_config = ConfigDict(populate_by_name=True)

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

    @classmethod
    def from_sekolah(
        cls,
        sekolah: "Sekolah",
    ) -> "EntitiSekolah":
        """Create an entity snapshot from a validated ``Sekolah`` model."""

        jumlah_pelajar = sum(
            value
            for value in (
                sekolah.enrolmenPrasekolah,
                sekolah.enrolmen,
                sekolah.enrolmenKhas,
            )
            if value is not None
        )

        jumlah_pelajar_enrolmen_khas = sekolah.enrolmenKhas if sekolah.enrolmenKhas is not None else 0
        jumlah_pelajar_tanpa_enrolmen_khas = jumlah_pelajar - jumlah_pelajar_enrolmen_khas

        location = None
        if sekolah.koordinatXX is not None and sekolah.koordinatYY is not None:
            location = GeoJSONPoint(coordinates=(sekolah.koordinatXX, sekolah.koordinatYY))

        info_sekolah = InfoSekolah(
            jenisLabel=sekolah.jenisLabel,
            jumlahPelajar=jumlah_pelajar,
            jumlahPelajarEnrolmenKhas=jumlah_pelajar_enrolmen_khas,
            jumlahPelajarTanpaEnrolmenKhas=jumlah_pelajar_tanpa_enrolmen_khas,
            jumlahGuru=sekolah.guru,
        )

        info_perhubungan = InfoKomunikasi(
            noTelefon=sekolah.noTelefon,
            noFax=sekolah.noFax,
            email=sekolah.email,
            alamatSurat=sekolah.alamatSurat,
            poskodSurat=str(sekolah.poskodSurat) if sekolah.poskodSurat is not None else None,
            bandarSurat=sekolah.bandarSurat,
        )

        profil_pentadbiran = InfoPentadbiran(
            negeri=sekolah.negeri,
            ppd=sekolah.ppd,
            parlimen=sekolah.parlimen,
            bantuan=sekolah.bantuan,
            bilSesi=sekolah.bilSesi,
            sesi=sekolah.sesi,
            prasekolah=sekolah.prasekolah,
            integrasi=sekolah.integrasi,
        )

        info_lokasi = InfoLokasi(
            koordinatXX=sekolah.koordinatXX,
            koordinatYY=sekolah.koordinatYY,
            location=location,
        )

        data = EntitiSekolahData(
            infoSekolah=info_sekolah,
            infoKomunikasi=info_perhubungan,
            infoPentadbiran=profil_pentadbiran,
            infoLokasi=info_lokasi,
        )

        status = sekolah.status if sekolah.status is not None else SekolahStatus.ACTIVE

        return cls(
            namaSekolah=sekolah.namaSekolah,
            kodSekolah=sekolah.kodSekolah,
            status=status,
            isSekolahAngkatMADANI=getattr(sekolah, "isSekolahAngkatMADANI", None),
            data=data,
            createdAt=_utc_now(),
        )

    def to_document(self) -> dict:
        """Convert the entity to a Mongo-ready document, omitting ``None`` fields."""
        document = self.model_dump(by_alias=True)

        try:
            location = document["data"]["infoLokasi"]["location"]
        except KeyError:
            location = None

        if location and "coordinates" in location:
            coords = location["coordinates"]
            if isinstance(coords, tuple):
                location["coordinates"] = list(coords)

        return document

__all__ = [
    "EntitiSekolah",
    "EntitiSekolahData",
    "InfoSekolah",
    "InfoKomunikasi",
    "InfoPentadbiran",
    "InfoLokasi",
    "GeoJSONPoint",
]