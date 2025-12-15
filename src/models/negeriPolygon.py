from typing import List, Optional, Any, ClassVar, Dict
from datetime import datetime, timezone

from pydantic import BaseModel, Field
from src.config.settings import get_settings
from src.models.negeriEnum import NegeriEnum


_settings = get_settings()


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


class NegeriPolygonCentroid(BaseModel):
    """Centroid representation for NegeriPolygon."""

    location: Optional[Dict[str, Any]] = Field(default=None, description="GeoJSON Point {type: 'Point', coordinates: [lon, lat]} for centroid of schools in this negeri")
    koordinatXX: Optional[float] = Field(default=None, description="Longitude (x) of centroid of schools in this negeri")
    koordinatYY: Optional[float] = Field(default=None, description="Latitude (y) of centroid of schools in this negeri")

class NegeriPolygon(BaseModel):
    """State polygon data model."""

    collection_name: ClassVar[str] = _settings.negeri_polygon_collection

    negeri: NegeriEnum = Field(..., description="Negeri name following NegeriEnum")
    geometry: Dict[str, Any] = Field(..., description="GeoJSON geometry (MultiPolygon)")
    centroid: Optional[NegeriPolygonCentroid] = Field(default=None, description="Centroid object containing GeoJSON location and koordinatXX/koordinatYY values",)
    updated_at: datetime = Field(default_factory=_utc_now, description="Last updated timestamp in UTC")

    def to_document(self) -> Dict[str, Any]:
        """Convert model to MongoDB document with _id = negeri value."""

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
            "_id": self.negeri.value,
            "negeri": self.negeri.value,
            "geometry": self.geometry,
            "centroid": centroid_doc,
            "updatedAt": self.updated_at,
        }