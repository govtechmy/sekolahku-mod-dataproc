from typing import List
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
    parlimen_list: List[str] = Field(default_factory=list, description="List of parliament names in this Negeri")
    geometry: Dict[str, Any] = Field(..., description="GeoJSON geometry (MultiPolygon)")
    updated_at: datetime = Field(default_factory=_utc_now, description="Last updated timestamp in UTC")

    def to_document(self) -> Dict[str, Any]:
        """Convert model to MongoDB document with _id = negeri value."""
        return {
            "_id": self.negeri.value,
            "negeri": self.negeri.value,
            "geometry": self.geometry,
            "updatedAt": self.updated_at
        }