from datetime import datetime, timezone
from typing import Optional
from pydantic import BaseModel, Field


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)

class NegeriPolygon(BaseModel):
    """
    Negeri (State) polygon seed data.
    
    Source: OpenDOSM Kawasanku dashboard
    Collection: negeri_polygon
    
    Note: negeri field is used as _id in MongoDB
    """
    code_state: Optional[int] = Field(None, description="State code from OpenDOSM")
    
    # GeoJSON geometry
    geometry: dict = Field(..., description="GeoJSON geometry (Polygon or MultiPolygon)")
    
    createdAt: datetime = Field(default_factory=_utc_now, description="UTC timestamp when the document was created")
    
    class Config:
        json_schema_extra = {
            "example": {
                "code_state": 1,
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [[...]]
                },
                "createdAt": "2024-12-04T00:00:00Z"
            }
        }


class ParliamentPolygon(BaseModel):
    """
    Parliament constituency polygon seed data.
    
    Source: OpenDOSM Kawasanku dashboard
    Collection: parliament_polygon
    
    Note: parlimen field is used as _id in MongoDB
    """
    negeri: str = Field(description="State name (e.g., 'JOHOR', 'PULAU_PINANG')")
    parlimen_code: str = Field(description="Parliament code (e.g., 'P.140')")
    parlimen_name: str = Field(description="Parliament name only (e.g., 'SEGAMAT')")
    parlimen_display: str = Field(description="Display name (e.g., 'P.140 Segamat')")
    
    code_state: Optional[int] = Field(description="State code from OpenDOSM")
    code_parlimen: Optional[str] = Field(description="Parliament code from OpenDOSM")
    
    # GeoJSON geometry
    geometry: dict = Field(..., description="GeoJSON geometry (Polygon or MultiPolygon)")
    
    createdAt: datetime = Field(default_factory=_utc_now, description="UTC timestamp when the document was created")

    
    class Config:
        json_schema_extra = {
            "example": {
                "negeri": "JOHOR",
                "parlimen_code": "P.140",
                "parlimen_name": "SEGAMAT",
                "parlimen_display": "P.140 Segamat",
                "code_state": 1,
                "code_parlimen": "P.140",
                "geometry": {
                    "type": "MultiPolygon",
                    "coordinates": [[[...]]]
                },
                "createdAt": "2024-12-04T00:00:00Z"
            }
        }
