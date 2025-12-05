"""Polygon models for storing state and parliament geometry."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import ClassVar, Dict, Any, List

from pydantic import BaseModel, Field

from src.config.settings import get_settings
from src.models.negeriEnum import NegeriEnum

_settings = get_settings()


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


class NegeriPolygon(BaseModel):
    """State polygon data model."""

    collection_name: ClassVar[str] = _settings.negeri_polygon_collection

    negeri: NegeriEnum = Field(..., description="State name following NegeriEnum")
    parlimen_list: List[str] = Field(default_factory=list, description="List of parliament names in this state")
    geometry: Dict[str, Any] = Field(..., description="GeoJSON geometry (MultiPolygon)")
    updated_at: datetime = Field(default_factory=_utc_now, description="Last updated timestamp in UTC")

    def to_document(self) -> Dict[str, Any]:
        """Convert model to MongoDB document with _id = negeri value."""
        return {
            "_id": self.negeri.value,
            "negeri": self.negeri.value,
            "parlimenList": self.parlimen_list,
            "geometry": self.geometry,
            "updatedAt": self.updated_at
        }


class ParlimenPolygon(BaseModel):
    """Parliament polygon data model."""

    collection_name: ClassVar[str] = _settings.parlimen_polygon_collection

    negeri: NegeriEnum = Field(..., description="State name following NegeriEnum")
    parlimen: str = Field(..., description="Parliament name")
    geometry: Dict[str, Any] = Field(..., description="GeoJSON geometry (MultiPolygon)")
    updated_at: datetime = Field(default_factory=_utc_now, description="Last updated timestamp in UTC")

    def to_document(self) -> Dict[str, Any]:
        """Convert model to MongoDB document with _id = negeri::parlimen."""
        _id = f"{self.negeri.value}::{self.parlimen}"
        return {
            "_id": _id,
            "negeri": self.negeri.value,
            "parlimen": self.parlimen,
            "geometry": self.geometry,
            "updatedAt": self.updated_at
        }
