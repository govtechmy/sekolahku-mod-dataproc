from typing import List, Optional
from datetime import datetime, timezone
from typing import Any, ClassVar, Dict

from pydantic import BaseModel, Field
from src.config.settings import get_settings
from src.models.negeriEnum import NegeriEnum


_settings = get_settings()

def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


class NegeriPolygon(BaseModel):
    """State polygon data model."""

    collection_name: ClassVar[str] = _settings.negeri_polygon_collection

    negeri: NegeriEnum = Field(..., description="Negeri name following NegeriEnum")
    geometry: Dict[str, Any] = Field(..., description="GeoJSON geometry (MultiPolygon)")
    centroid: Optional[Dict[str, Any]] = Field(default=None, description="GeoJSON Point representing centroid of schools in this negeri")
    centroidXX: Optional[float] = Field(default=None, description="Longitude of centroid of schools in this negeri")
    centroidYY: Optional[float] = Field(default=None, description="Latitude of centroid of schools in this negeri")
    updated_at: datetime = Field(default_factory=_utc_now, description="Last updated timestamp in UTC")

    def to_document(self) -> Dict[str, Any]:
        """Convert model to MongoDB document with _id = negeri value."""
        return {
            "_id": self.negeri.value,
            "negeri": self.negeri.value,
            "geometry": self.geometry,
            "centroid": self.centroid,
            "centroidXX": self.centroidXX,
            "centroidYY": self.centroidYY,
            "updatedAt": self.updated_at
        }