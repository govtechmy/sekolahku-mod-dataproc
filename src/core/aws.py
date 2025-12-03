import boto3

from src.config.settings import get_settings

def get_s3_client():
    return boto3.client("s3")

def get_s3_bucket_name() -> str:
    return get_settings().s3_bucket_name