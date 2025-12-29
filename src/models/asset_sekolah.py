from typing import Optional, Any, ClassVar, Dict
from datetime import datetime

from pydantic import BaseModel, Field
from src.config.settings import get_settings
from src.service.assets.helpers import _utc_now


_settings = get_settings()


class S3Urls(BaseModel):
    """S3 URLs for various school assets."""
    
    logo: Optional[str] = Field(default=None, description="S3 URL for logo image")
    gallery: Optional[str] = Field(default=None, description="S3 URL for gallery images")
    hero: Optional[str] = Field(default=None, description="S3 URL for hero image")


class AssetSekolah(BaseModel):
    """School assets metadata model.

    This is a metadata collection that stores S3 URLs for school assets (logos, images, JSON files).
    Every school in the Sekolah collection gets an entry, regardless of whether they have assets available.
    """
    
    collection_name: ClassVar[str] = _settings.asset_sekolah_collection
    
    kodSekolah: str = Field(..., description="School code (matches Sekolah._id)")
    status: Optional[str] = Field(default=None, description="School status from Sekolah collection")
    s3Url: S3Urls = Field(..., description="S3 URLs for all asset types")
    updatedAt: datetime = Field(default_factory=_utc_now, description="Last updated timestamp in UTC")
    
    def to_document(self) -> Dict[str, Any]:
        """Convert model to MongoDB document with _id = kodSekolah value."""
        
        return {
            "_id": self.kodSekolah,
            "kodSekolah": self.kodSekolah,
            "status": self.status,
            "s3Url": {
                "logo": self.s3Url.logo,
                "gallery": self.s3Url.gallery,
                "hero": self.s3Url.hero,
            },
            "updatedAt": self.updatedAt,
        }
