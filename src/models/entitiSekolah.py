from __future__ import annotations

from typing import ClassVar, Optional, TYPE_CHECKING

from datetime import datetime, timezone

from pydantic import BaseModel, EmailStr, Field
from pydantic import ConfigDict
from typing_extensions import Literal


if TYPE_CHECKING:  # pragma: no cover - imported for type checking only
    from src.models.sekolah import Sekolah

def _utc_now() -> datetime:
    return datetime.now(timezone.utc)

class InfoSekolah(BaseModel):
    jenisLabel: Optional[str] = Field(default=None, description="Type of school label e.g SK, SMK, SMKA, etc.")
    jumlahPelajar: Optional[int] = Field(default=0, description="Total students (enrolmenPrasekolah + enrolmen + enrolmenKhas)")
    jumlahGuru: Optional[int] = Field(default=0, description="Total number of teachers")

class InfoKomunikasi(BaseModel):
    noTelefon: Optional[str] = Field(default=None, description="Primary contact number")
    noFax: Optional[str] = Field(default=None, description="Fax number")
    email: Optional[EmailStr] = Field(default=None, description="General contact email")
    alamatSurat: Optional[str] = Field(default=None, description="Mailing address")
    poskodSurat: Optional[str] = Field(default=None, description="Mailing postcode")
    bandarSurat: Optional[str] = Field(default=None, description="Mailing city")

class InfoPentadbiran(BaseModel):
    negeri: Optional[str] = Field(default=None, description="State the school is located in")
    ppd: Optional[str] = Field(default=None, description="Pejabat Pendidikan Daerah (district office)")
    parlimen: Optional[str] = Field(default=None, description="Parliament constituency")
    bantuan: Optional[str] = Field(default=None, description="Bantuan classification")
    bilSesi: Optional[str] = Field(default=None, description="Number of school sessions")
    sesi: Optional[str] = Field(default=None, description="School session")
    prasekolah: Optional[bool] = Field(default=None, description="Has preschool programme")
    integrasi: Optional[bool] = Field(default=None, description="Runs integration programme")

class GeoJSONPoint(BaseModel):
    """Minimal GeoJSON point structure used for school coordinates."""

    type: Literal["Point"] = Field(default="Point", description="GeoJSON geometry type")
    coordinates: tuple[float, float] = Field(..., description="(longitude, latitude) coordinate pair")


class InfoLokasi(BaseModel):
    koordinatXX: Optional[float] = Field(default=None, description="Longitude value")
    koordinatYY: Optional[float] = Field(default=None, description="Latitude value")
    location: Optional[GeoJSONPoint] = Field(default=None, description="GeoJSON point for geospatial queries")


class SekolahBerdekatanItem(BaseModel):
    namaSekolah: Optional[str] = Field(default=None, description="Name of the nearby school")
    kodSekolah: str = Field(..., description="Unique school code identifier for the nearby school")
    bandarSurat: Optional[str] = Field(default=None, description="Mailing city of the nearby school")
    negeri: Optional[str] = Field(default=None, description="State of the nearby school")


class SekolahBerdekatan(BaseModel):
    senarai: list[SekolahBerdekatanItem] = Field(default_factory=list, description="List of nearby schools")


class EntitiSekolahData(BaseModel):
    infoSekolah: InfoSekolah
    infoKomunikasi: InfoKomunikasi
    infoPentadbiran: InfoPentadbiran
    infoLokasi: InfoLokasi
    sekolahBerdekatan: SekolahBerdekatan = Field(default_factory=SekolahBerdekatan, description="Nearby schools derived from order priority: bandarSurat -> dun -> parlimen -> ppd -> negeri")

class EntitiSekolah(BaseModel):
    """Aggregated entity view for a school document."""

    collection_name: ClassVar[str] = "EntitiSekolah"

    namaSekolah: Optional[str] = Field(default=None, description="Name of the school")
    kodSekolah: str = Field(..., description="Unique school code identifier")
    data: EntitiSekolahData
    updatedAt: datetime = Field(default_factory=_utc_now, description="UTC timestamp when the document was last generated",)

    @classmethod
    def from_sekolah(
        cls,
        sekolah: "Sekolah",
        *,
        sekolah_berdekatan: Optional[SekolahBerdekatan] = None,
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

        location = None
        if sekolah.koordinatXX is not None and sekolah.koordinatYY is not None:
            location = GeoJSONPoint(coordinates=(sekolah.koordinatXX, sekolah.koordinatYY))

        info_sekolah = InfoSekolah(
            jenisLabel=sekolah.jenisLabel,
            jumlahPelajar=jumlah_pelajar,
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
            sekolahBerdekatan=sekolah_berdekatan or SekolahBerdekatan(),
        )

        return cls(
            namaSekolah=sekolah.namaSekolah,
            kodSekolah=sekolah.kodSekolah,
            data=data,
            updatedAt=_utc_now(),
        )

    def to_document(self) -> dict:
        """Convert the entity to a Mongo-ready document, omitting ``None`` fields."""
        return self.model_dump(exclude_none=True, by_alias=True)

__all__ = [
    "EntitiSekolah",
    "EntitiSekolahData",
    "InfoSekolah",
    "InfoKomunikasi",
    "InfoPentadbiran",
    "InfoLokasi",
    "GeoJSONPoint",
    "SekolahBerdekatan",
    "SekolahBerdekatanItem",
]