from __future__ import annotations

from typing import ClassVar, Optional, TYPE_CHECKING
from collections import defaultdict
from datetime import datetime

from pydantic import BaseModel, Field

if TYPE_CHECKING:  # pragma: no cover - imported for type checking only
    from src.models.sekolah import Sekolah


class AnalitikSekolahData(BaseModel):
    """Dynamic analytics container that captures all categories found in data."""
    
    byPeringkat: dict[str, int] = Field(default_factory=dict, description="Education level breakdown with JUMLAH total")
    byJenisLabel: dict[str, int] = Field(default_factory=dict, description="Sekolah type breakdown with JUMLAH total") 
    bySesi: dict[str, int] = Field(default_factory=dict, description="Session type breakdown with JUMLAH total")
    byBantuan: dict[str, int] = Field(default_factory=dict, description="Aid type breakdown with JUMLAH total")
    byLokasi: dict[str, int] = Field(default_factory=dict, description="Location type breakdown with JUMLAH total")


class AnalitikSekolah(BaseModel):
    """Analytics aggregation snapshot created from Sekolah collection data."""

    collection_name: ClassVar[str] = "AnalitikSekolah"

    region: Optional[str] = Field(default="ALL", description="Region scope for analytics (e.g., state, district, or ALL for national)")
    data: AnalitikSekolahData
    jumlahSekolah: int = Field(default=0, description="Total number of Sekolah processed")
    updatedAt: datetime = Field(default_factory=datetime.utcnow, description="UTC timestamp when the analytics were computed")

    @classmethod
    def from_sekolah_list(cls, sekolah_list: list["Sekolah"], *, region: str = "ALL") -> "AnalitikSekolah":
        """Create an analytics snapshot from a list of validated ``Sekolah`` models."""
        
        jumlah_sekolah = len(sekolah_list)
        
        # Initialize analytics containers with defaultdicts for dynamic counting
        peringkat_counts = defaultdict(int)
        jenis_counts = defaultdict(int)
        sesi_counts = defaultdict(int)
        bantuan_counts = defaultdict(int)
        lokasi_counts = defaultdict(int)

        # Process each sekolah to build analytics dynamically
        for sekolah in sekolah_list:
            cls._increment_count(peringkat_counts, sekolah.peringkat)
            cls._increment_count(jenis_counts, sekolah.jenisLabel)
            cls._increment_count(sesi_counts, sekolah.sesi)
            cls._increment_count(bantuan_counts, sekolah.bantuan)
            cls._increment_count(lokasi_counts, sekolah.lokasi)

        # Convert defaultdict to regular dict and add JUMLAH totals
        data = AnalitikSekolahData(
            byPeringkat=cls._finalize_counts(peringkat_counts, jumlah_sekolah),
            byJenisLabel=cls._finalize_counts(jenis_counts, jumlah_sekolah),
            bySesi=cls._finalize_counts(sesi_counts, jumlah_sekolah),
            byBantuan=cls._finalize_counts(bantuan_counts, jumlah_sekolah),
            byLokasi=cls._finalize_counts(lokasi_counts, jumlah_sekolah),
        )

        return cls(
            region=region,
            data=data,
            jumlahSekolah=jumlah_sekolah,
            updatedAt=datetime.utcnow(),
        )

    @staticmethod
    def _increment_count(counter: dict, value: Optional[str]) -> None:
        """Increment the count for a given value in a dynamic counter."""
        key = AnalitikSekolah._normalize_value(value)
        counter[key] = counter.get(key, 0) + 1

    @staticmethod
    def _finalize_counts(counter: defaultdict, total: int) -> dict[str, int]:
        """Convert defaultdict to regular dict and add JUMLAH total."""
        final_counts = dict(counter)
        
        # Ensure TIADA MAKLUMAT exists even if count is 0
        if "TIADA MAKLUMAT" not in final_counts:
            final_counts["TIADA MAKLUMAT"] = 0
            
        # Add JUMLAH total
        final_counts["JUMLAH"] = total
        
        return final_counts

    @staticmethod
    def _normalize_value(value: Optional[str]) -> str:
        """Normalize field values for consistent analytics categorization."""
        if value is None or str(value).strip() == "":
            return "TIADA MAKLUMAT"
        return str(value).strip().upper()

    def to_document(self) -> dict:
        """Convert the analytics to a Mongo-ready document, omitting ``None`` fields."""
        return self.model_dump(exclude_none=True, by_alias=True)


__all__ = [
    "AnalitikSekolah",
    "AnalitikSekolahData",
]
