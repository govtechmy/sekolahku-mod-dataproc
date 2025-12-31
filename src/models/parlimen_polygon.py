import datetime
from typing import Any, ClassVar, Dict, Optional
from datetime import datetime, timezone
from src.core.time import _utc_now

from pydantic import BaseModel, Field
from src.config.settings import get_settings
from src.models.negeri_enum import NegeriEnum

_settings = get_settings()


class ParlimenPolygonCentroid(BaseModel):
    """Centroid representation for ParlimenPolygon."""

    location: Optional[Dict[str, Any]] = Field(default=None, description="GeoJSON Point {type: 'Point', coordinates: [lon, lat]} for centroid of schools in this negeri-parlimen")
    koordinatXX: Optional[float] = Field(default=None, description="Longitude (x) of centroid of schools in this negeri-parlimen")
    koordinatYY: Optional[float] = Field(default=None, description="Latitude (y) of centroid of schools in this negeri-parlimen",)

class ParlimenPolygon(BaseModel):
    """Parliament polygon data model."""

    collection_name: ClassVar[str] = _settings.parlimen_polygon_collection

    negeri: NegeriEnum = Field(..., description="Negeri name following NegeriEnum")
    parlimen: str = Field(..., description="Parliament name")
    geometry: Dict[str, Any] = Field(..., description="GeoJSON geometry (MultiPolygon)")
    centroid: Optional[ParlimenPolygonCentroid] = Field(default=None, description="Centroid object containing GeoJSON location and koordinatXX/koordinatYY values",)
    updated_at: datetime = Field(default_factory=_utc_now, description="Last updated timestamp in UTC")

    def to_document(self) -> Dict[str, Any]:
        """Convert model to MongoDB document with _id = negeri::parlimen."""
        _id = f"{self.negeri.value}::{self.parlimen}"

        centroid_doc: Dict[str, Any] | None
        if self.centroid is not None:
            centroid_doc = {
                "location": self.centroid.location,
                "koordinatXX": self.centroid.koordinatXX,
                "koordinatYY": self.centroid.koordinatYY,
            }
        else:
            centroid_doc = None

        return {
            "_id": _id,
            "negeri": self.negeri.value,
            "parlimen": self.parlimen,
            "geometry": self.geometry,
            "centroid": centroid_doc,
            "updatedAt": self.updated_at,
        }
