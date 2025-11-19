from __future__ import annotations

from typing import Dict
from datetime import datetime
from pydantic import BaseModel, Field, ConfigDict


class StatistikSekolah(BaseModel):
    jumlahSekolah: int = Field(..., description="Total number of schools")
    bantuan: Dict[str, int] = Field(default_factory=dict, description="Total by type of bantuan")
    bilSesi: Dict[str, int] = Field(default_factory=dict, description="Total by number of school sessions")
    lokasi: Dict[str, int] = Field(default_factory=dict, description="Total by location")


class StatistikGuru(BaseModel):
    jumlahGuru: int = Field(..., description="Total number of teachers")
    jantina: Dict[str, int] = Field(default_factory=dict, description="Total of teachers by gender")


class StatistikMurid(BaseModel):
    jumlahMurid: int = Field(..., description="Total number of students")
    jantina: Dict[str, int] = Field(default_factory=dict, description="Total of students by gender")


class StatistikSekolahDocument(BaseModel):
    """Summary document for the StatistikSekolah collection."""

    data: StatistikSekolah
    updatedAt: datetime = Field(
        default_factory=datetime.utcnow,
        description="UTC timestamp when statistik were last updated",
    )

class StatistikGuruDocument(BaseModel):
    """Summary document for the StatistikGuru collection."""

    data: StatistikGuru
    updatedAt: datetime = Field(
        default_factory=datetime.utcnow,
        description="UTC timestamp when statistik were last updated",
    )

class StatistikMuridDocument(BaseModel):
    """Summary document for the StatistikMurid collection."""

    data: StatistikMurid
    updatedAt: datetime = Field(
        default_factory=datetime.utcnow,
        description="UTC timestamp when statistik were last updated",
    )

class StatistikSummary(BaseModel):
    sekolah: StatistikSekolah
    guru: StatistikGuru
    murid: StatistikMurid

    def as_dict(self) -> dict:
        return {
            "sekolah": {"data": self.sekolah.model_dump()},
            "guru": {"data": self.guru.model_dump()},
            "murid": {"data": self.murid.model_dump()},
        }
