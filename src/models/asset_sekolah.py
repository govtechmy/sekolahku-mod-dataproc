from typing import Optional, Any, ClassVar, Dict
from datetime import datetime

from pydantic import BaseModel, Field
from src.config.settings import get_settings
from src.service.assets.helpers import _utc_now


_settings = get_settings()


class S3Urls(BaseModel):
    """S3 URLs for various school assets."""
    
    json: Optional[str] = Field(default=None, description="S3 URL for JSON data")
    logo: Optional[str] = Field(default=None, description="S3 URL for logo image")
    gallery: Optional[str] = Field(default=None, description="S3 URL for gallery images")
    hero: Optional[str] = Field(default=None, description="S3 URL for hero image")


class AssetSekolah(BaseModel):
    """School assets data model.
    
    This collection stores S3 URLs for school assets (logos, images, JSON files).
    Every school in the Sekolah collection gets an entry, regardless of whether
    they have assets available.

    """
    
    collection_name: ClassVar[str] = _settings.asset_sekolah_collection
    
    kod_sekolah: str = Field(..., description="School code (matches Sekolah._id)")
    status: Optional[str] = Field(default=None, description="School status from Sekolah collection")
    s3_urls: S3Urls = Field(..., description="S3 URLs for all asset types")
    updated_at: datetime = Field(default_factory=_utc_now, description="Last updated timestamp in UTC")
    
    def to_document(self) -> Dict[str, Any]:
        """Convert model to MongoDB document with _id = kodSekolah value."""
        
        return {
            "_id": self.kod_sekolah,
            "status": self.status,
            "s3_urls": {
                "json": self.s3_urls.json,
                "logo": self.s3_urls.logo,
                "gallery": self.s3_urls.gallery,
                "hero": self.s3_urls.hero,
            },
            "updatedAt": self.updated_at,
        }
