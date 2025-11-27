from __future__ import annotations

from typing import ClassVar, Optional, TYPE_CHECKING
from collections import defaultdict
from datetime import datetime, timezone

from pydantic import BaseModel, Field
from src.config.settings import get_settings

if TYPE_CHECKING:  # pragma: no cover - imported for type checking only
    from src.models.sekolah import Sekolah


def _utc_now() -> datetime:
    """Return timezone-aware UTC datetime."""
    return datetime.now(timezone.utc)


class AnalitikItem(BaseModel):
    """Individual analytics item with type, percentage, and total."""
    
    jenis: str = Field(..., description="Kategori atau jenis")
    peratus: float = Field(..., description="Peratusan daripada jumlah keseluruhan")
    total: int = Field(..., description="Jumlah bilangan untuk kategori ini")


class AnalitikSekolahData(BaseModel):
    """Container for all analytics dimensions in array format."""
    
    jenisLabel: list[AnalitikItem] = Field(default_factory=list, description="Analisis mengikut jenis/label sekolah") 
    bantuan: list[AnalitikItem] = Field(default_factory=list, description="Analisis mengikut jenis bantuan")


_settings = get_settings()


class AnalitikSekolah(BaseModel):
    """Analytics aggregation snapshot created from Sekolah collection data."""

    collection_name: ClassVar[str] = _settings.analitik_sekolah_collection

    jumlahSekolah: int = Field(default=0, description="Jumlah keseluruhan sekolah yang diproses")
    jumlahGuru: int = Field(default=0, description="Jumlah keseluruhan guru")
    jumlahPelajar: int = Field(default=0, description="Jumlah keseluruhan pelajar")
    data: AnalitikSekolahData
    createdAt: datetime = Field(default_factory=_utc_now, description="Masa analisis dijana")
    updatedAt: Optional[datetime] = Field(default=None, description="Masa analisis dikemas kini")

    @classmethod
    def from_sekolah_list(cls, sekolah_list: list["Sekolah"], *, region: str = "ALL") -> "AnalitikSekolah":
        """Create an analytics snapshot from a list of validated Sekolah models."""
        
        jumlah_sekolah = len(sekolah_list)
        
        # Calculate totals
        total_guru = 0
        total_pelajar = 0
        
        # Initialize analytics containers with defaultdicts for dynamic counting
        jenis_counts = defaultdict(int)
        bantuan_counts = defaultdict(int)

        # Process each sekolah to build analytics dynamically
        for sekolah in sekolah_list:
            # Count categories
            cls._increment_count(jenis_counts, sekolah.jenisLabel)
            cls._increment_count(bantuan_counts, sekolah.bantuan)
            
            # Sum totals
            if sekolah.guru is not None:
                total_guru += sekolah.guru
                
            # Calculate total students (enrolmen + enrolmenPrasekolah + enrolmenKhas)
            student_count = 0
            if sekolah.enrolmen is not None:
                student_count += sekolah.enrolmen
            if sekolah.enrolmenPrasekolah is not None:
                student_count += sekolah.enrolmenPrasekolah
            if sekolah.enrolmenKhas is not None:
                student_count += sekolah.enrolmenKhas
            total_pelajar += student_count

        # Convert counts to AnalitikItem arrays
        data = AnalitikSekolahData(
            jenisLabel=cls._convert_to_analitik_items(jenis_counts, jumlah_sekolah),
            bantuan=cls._convert_to_analitik_items(bantuan_counts, jumlah_sekolah),
        )

        created_at = _utc_now()

        return cls(
            jumlahSekolah=jumlah_sekolah,
            jumlahGuru=total_guru,
            jumlahPelajar=total_pelajar,
            data=data,
            createdAt=created_at,
            updatedAt=created_at,
        )

    @staticmethod
    def _increment_count(counter: dict, value: Optional[str]) -> None:
        """Increment the count for a given value in a dynamic counter."""
        key = AnalitikSekolah._normalize_value(value)
        counter[key] = counter.get(key, 0) + 1

    @staticmethod
    def _convert_to_analitik_items(counter: defaultdict, total: int) -> list[AnalitikItem]:
        """Convert defaultdict to list of AnalitikItem objects."""
        items = []
        
        # Add existing categories
        for jenis, count in counter.items():
            if total > 0:
                peratus = round((count / total) * 100, 1)
            else:
                peratus = 0.0
            
            items.append(AnalitikItem(
                jenis=jenis,
                peratus=peratus,
                total=count
            ))
        
        # Ensure "Tiada Maklumat" exists even if count is 0
        tiada_maklumat_exists = any(item.jenis == "TIADA MAKLUMAT" for item in items)
        if not tiada_maklumat_exists:
            items.append(AnalitikItem(
                jenis="TIADA MAKLUMAT",
                peratus=0.0,
                total=0
            ))
        
        # Sort by total count descending
        items.sort(key=lambda x: x.total, reverse=True)
        return items

    @staticmethod
    def _normalize_value(value: Optional[str]) -> str:
        """Normalize field values for consistent analytics categorization."""
        if value is None or str(value).strip() == "":
            return "TIADA MAKLUMAT"
        return str(value).strip().upper()

    def to_document(self) -> dict:
        """Convert the analytics to a Mongo-ready document, omitting None fields."""
        updated_at = self.updatedAt or self.createdAt
        document = self.model_dump(exclude_none=True, by_alias=True)
        document["updatedAt"] = updated_at
        return document


__all__ = [
    "AnalitikSekolah",
    "AnalitikSekolahData",
    "AnalitikItem",
]
