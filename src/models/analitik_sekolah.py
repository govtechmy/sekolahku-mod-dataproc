from __future__ import annotations

from typing import ClassVar, Optional, TYPE_CHECKING
from collections import defaultdict
from pydantic import BaseModel, Field
from src.config.settings import get_settings

if TYPE_CHECKING:  # pragma: no cover - imported for type checking only
    from src.models.sekolah import Sekolah


from datetime import datetime, timezone
from src.core.time import _utc_now


class PeringkatItem(BaseModel):
    """Breakdown of peringkat distribution within a jenis category."""
    
    peringkat: str = Field(..., description="Peringkat (e.g., Rendah, Menengah)")
    total: int = Field(..., description="Jumlah sekolah untuk peringkat ini dalam jenis ini")


class AnalitikJenisItem(BaseModel):
    """Analytics item for jenisLabel dimension."""
    
    jenis: str = Field(..., description="Kategori atau jenis")
    peratus: float = Field(..., description="Peratusan daripada jumlah keseluruhan")
    total: int = Field(..., description="Jumlah bilangan untuk kategori ini")
    peringkatBreakdown: list[PeringkatItem] = Field(default_factory=list, description="Breakdown mengikut peringkat dalam jenis ini")


class AnalitikBantuanItem(BaseModel):
    """Analytics item for bantuan dimension."""
    
    jenis: str = Field(..., description="Kategori atau jenis")
    peratus: float = Field(..., description="Peratusan daripada jumlah keseluruhan")
    total: int = Field(..., description="Jumlah bilangan untuk kategori ini")


class AnalitikSekolahData(BaseModel):
    """Container for all analytics dimensions in array format."""
    
    jenisLabel: list[AnalitikJenisItem] = Field(default_factory=list, description="Analisis mengikut jenis/label sekolah") 
    bantuan: list[AnalitikBantuanItem] = Field(default_factory=list, description="Analisis mengikut jenis bantuan")


_settings = get_settings()


class AnalitikSekolah(BaseModel):
    """Analytics aggregation snapshot created from Sekolah collection data."""

    collection_name: ClassVar[str] = _settings.analitik_sekolah_collection

    jumlahSekolah: int = Field(default=0, description="Jumlah keseluruhan sekolah yang diproses")
    jumlahGuru: int = Field(default=0, description="Jumlah keseluruhan guru")
    jumlahPelajar: int = Field(default=0, description="Jumlah keseluruhan pelajar")
    data: AnalitikSekolahData
    createdAt: datetime = Field(default_factory=_utc_now, description="Masa analisis dijana")
    updatedAt: Optional[datetime] = Field(default=None, description="Waktu kemaskini terakhir bagi dokumen analitik")

    @classmethod
    def from_sekolah_list(cls, sekolah_list: list["Sekolah"]) -> "AnalitikSekolah":
        """Create an analytics snapshot from a list of validated Sekolah models."""
        
        jumlah_sekolah = len(sekolah_list)
        
        # Calculate totals
        total_guru = 0
        total_pelajar = 0
        
        # Initialize analytics containers with defaultdicts for dynamic counting
        jenis_counts = defaultdict(int)
        jenis_peringkat_counts = defaultdict(lambda: defaultdict(int))
        bantuan_counts = defaultdict(int)

        # Process each sekolah to build analytics dynamically
        for sekolah in sekolah_list:
            # Count categories
            jenis_key = cls._normalize_value(sekolah.jenisLabel)
            peringkat_key = cls._normalize_peringkat_value(sekolah.peringkat)
            jenis_counts[jenis_key] += 1
            jenis_peringkat_counts[jenis_key][peringkat_key] += 1
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

        # Convert counts to analytics arrays
        data = AnalitikSekolahData(
            jenisLabel=cls._convert_to_analitik_jenis_items(
                jenis_counts,
                jumlah_sekolah,
                jenis_peringkat_counts=jenis_peringkat_counts,
            ),
            bantuan=cls._convert_to_analitik_bantuan_items(bantuan_counts, jumlah_sekolah),
        )

        return cls(
            jumlahSekolah=jumlah_sekolah,
            jumlahGuru=total_guru,
            jumlahPelajar=total_pelajar,
            data=data,
            createdAt=_utc_now(),
        )

    @staticmethod
    def _increment_count(counter: dict, value: Optional[str]) -> None:
        """Increment the count for a given value in a dynamic counter."""
        key = AnalitikSekolah._normalize_value(value)
        counter[key] = counter.get(key, 0) + 1

    @staticmethod
    def _convert_to_analitik_jenis_items(
        counter: defaultdict,
        total: int,
        jenis_peringkat_counts: Optional[dict[str, dict]] = None,
    ) -> list[AnalitikJenisItem]:
        """Convert defaultdict to list of AnalitikJenisItem objects."""
        items = []
        
        # Add existing categories
        for jenis, count in counter.items():
            if total > 0:
                peratus = round((count / total) * 100, 1)
            else:
                peratus = 0.0
            
            # Build peringkat breakdown for this jenis if data is available
            peringkat_breakdown = []
            if jenis_peringkat_counts and jenis in jenis_peringkat_counts:
                peringkat_counts = jenis_peringkat_counts[jenis]
                for peringkat_key, peringkat_count in peringkat_counts.items():
                    if peringkat_key != "TIADA MAKLUMAT":  # Only include actual peringkat values
                        peringkat_breakdown.append(
                            PeringkatItem(
                                peringkat=AnalitikSekolah._display_peringkat(peringkat_key),
                                total=peringkat_count
                            )
                        )
                # Add TIADA MAKLUMAT if it exists
                if "TIADA MAKLUMAT" in peringkat_counts and peringkat_counts["TIADA MAKLUMAT"] > 0:
                    peringkat_breakdown.append(
                        PeringkatItem(
                            peringkat="TIADA MAKLUMAT",
                            total=peringkat_counts["TIADA MAKLUMAT"]
                        )
                    )
                # Sort by total descending
                peringkat_breakdown.sort(key=lambda x: x.total, reverse=True)
            
            items.append(AnalitikJenisItem(
                jenis=jenis,
                peratus=peratus,
                total=count,
                peringkatBreakdown=peringkat_breakdown
            ))
        
        # Ensure "Tiada Maklumat" exists even if count is 0
        tiada_maklumat_exists = any(item.jenis == "TIADA MAKLUMAT" for item in items)
        if not tiada_maklumat_exists:
            items.append(AnalitikJenisItem(
                jenis="TIADA MAKLUMAT",
                peratus=0.0,
                total=0,
                peringkatBreakdown=[]
            ))
        
        # Sort by total count descending
        items.sort(key=lambda x: x.total, reverse=True)
        return items

    @staticmethod
    def _convert_to_analitik_bantuan_items(
        counter: defaultdict,
        total: int,
    ) -> list[AnalitikBantuanItem]:
        """Convert defaultdict to list of AnalitikBantuanItem objects."""
        items = []

        for jenis, count in counter.items():
            if total > 0:
                peratus = round((count / total) * 100, 1)
            else:
                peratus = 0.0

            items.append(AnalitikBantuanItem(
                jenis=jenis,
                peratus=peratus,
                total=count,
            ))

        tiada_maklumat_exists = any(item.jenis == "TIADA MAKLUMAT" for item in items)
        if not tiada_maklumat_exists:
            items.append(AnalitikBantuanItem(
                jenis="TIADA MAKLUMAT",
                peratus=0.0,
                total=0,
            ))

        items.sort(key=lambda x: x.total, reverse=True)
        return items

    @staticmethod
    def _normalize_value(value: Optional[str]) -> str:
        """Normalize field values for consistent analytics categorization."""
        if value is None or str(value).strip() == "":
            return "TIADA MAKLUMAT"
        return str(value).strip().upper()

    @classmethod
    def _normalize_peringkat_value(cls, value: Optional[str]) -> str:
        if value is not None and hasattr(value, "value"):
            value = getattr(value, "value")
        normalized = cls._normalize_value(value)
        if normalized in {"RENDAH", "MENENGAH"}:
            return normalized
        return "TIADA MAKLUMAT"

    @classmethod
    def _display_peringkat(cls, value: str) -> str:
        normalized = cls._normalize_peringkat_value(value)
        if normalized == "RENDAH":
            return "Rendah"
        if normalized == "MENENGAH":
            return "Menengah"
        return "TIADA MAKLUMAT"

    def to_document(self) -> dict:
        """Convert the analytics to a Mongo-ready document, omitting None fields."""
        doc = self.model_dump(exclude_none=True, by_alias=True)
        doc["updatedAt"] = _utc_now()
        return doc


__all__ = [
    "AnalitikSekolah",
    "AnalitikSekolahData",
    "AnalitikJenisItem",
    "AnalitikBantuanItem",
    "PeringkatItem",
]
