from __future__ import annotations

from datetime import datetime, timezone
from src.core.time import _utc_now
from typing import ClassVar, List, Literal, Tuple

from pydantic import BaseModel, ConfigDict, Field

from src.config.settings import get_settings


_settings = get_settings()



class GeoJSONPolygon(BaseModel):
    """GeoJSON Polygon/MultiPolygon representation for region boundary."""

    type: Literal["Polygon", "MultiPolygon"] = Field(..., description="GeoJSON geometry type")
    coordinates: List = Field(..., description="GeoJSON coordinates array")


class GeoJSONPoint(BaseModel):
    """GeoJSON Point representation used for centroid.location."""

    type: Literal["Point"] = Field("Point", description="GeoJSON geometry type")
    coordinates: Tuple[float, float] = Field(..., description="(longitude, latitude) coordinate pair")


class Centroid(BaseModel):
    """Structured centroid with raw coordinates and GeoJSON location."""

    location: GeoJSONPoint = Field(..., description="GeoJSON point for centroid location")
    koordinatXX: float = Field(..., description="Longitude value of centroid")
    koordinatYY: float = Field(..., description="Latitude value of centroid")


class MalaysiaPolygon(BaseModel):
    """Region-level Malaysia polygon document.

    Represents a dissolved polygon for either West or East Malaysia, with centroid.
    """
    collection_name: ClassVar[str] = "MalaysiaPolygon"

    region: str = Field(..., description="Region identifier, e.g. WEST_MALAYSIA or EAST_MALAYSIA")
    geometry: GeoJSONPolygon = Field(..., description="Region boundary geometry as GeoJSON")
    centroid: Centroid = Field(..., description="Centroid information including coordinates and GeoJSON point")
    createdAt: datetime = Field(default_factory=_utc_now, description="UTC timestamp when the document was created")

    model_config = ConfigDict(populate_by_name=True)

    def to_document(self) -> dict:
        """Convert the model to a Mongo-ready document, omitting ``None`` values."""

        document = self.model_dump(exclude_none=True, by_alias=True)

        coords = document["centroid"]["location"]["coordinates"]
        if isinstance(coords, tuple):
            document["centroid"]["location"]["coordinates"] = list(coords)

        return document


__all__ = [
    "MalaysiaPolygon",
    "GeoJSONPolygon",
    "GeoJSONPoint",
    "Centroid",
]
