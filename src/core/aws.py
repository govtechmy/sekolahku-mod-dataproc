import boto3

from src.config.settings import get_settings

settings = get_settings()
def get_s3_client():
    return boto3.client("s3", bucket_name=settings.s3_bucket_name)