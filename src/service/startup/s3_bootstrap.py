from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import List, Tuple

import boto3
from botocore.exceptions import ClientError

from src.config.settings import Settings, get_settings

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class S3Check:
    name: str
    bucket: str | None
    key_or_prefix: str
    min_objects: int = 1
    is_exact_key: bool = False


@dataclass
class CheckResult:
    name: str
    found: int
    required: int


def _count_objects(client, check: S3Check) -> int:
    if not check.bucket:
        logger.warning("Skipping S3 check '%s' because bucket is not configured", check.name)
        return 0

    if check.is_exact_key:
        try:
            client.head_object(Bucket=check.bucket, Key=check.key_or_prefix)
            return 1
        except ClientError as exc:
            code = exc.response.get("Error", {}).get("Code")
            if code in ("404", "NoSuchKey"):
                return 0
            logger.warning(
                "Error while checking S3 object for '%s' at s3://%s/%s: %s",
                check.name,
                check.bucket,
                check.key_or_prefix,
                exc,
            )
            return 0
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "Unexpected error while checking S3 object for '%s' at s3://%s/%s: %s",
                check.name,
                check.bucket,
                check.key_or_prefix,
                exc,
            )
            return 0

    try:
        paginator = client.get_paginator("list_objects_v2")
        total = 0
        for page in paginator.paginate(Bucket=check.bucket, Prefix=check.key_or_prefix):
            total += len(page.get("Contents", []))
        return total
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "Error while listing S3 prefix for '%s' at s3://%s/%s: %s",
            check.name,
            check.bucket,
            check.key_or_prefix,
            exc,
        )
        return 0


def _build_checks(settings: Settings) -> List[S3Check]:
    return [
        S3Check(
            name="raw_opendosm_negeri",
            bucket=settings.s3_bucket_dataproc,
            key_or_prefix=f"{settings.s3_prefix_opendosm}/negeri/",
        ),
        S3Check(
            name="raw_opendosm_parlimen",
            bucket=settings.s3_bucket_dataproc,
            key_or_prefix=f"{settings.s3_prefix_opendosm}/parlimen/",
        ),
        S3Check(
            name="polygon_exports",
            bucket=settings.s3_bucket_public,
            key_or_prefix=f"{settings.s3_prefix_polygon}/",
        ),
        S3Check(
            name="centroid_manifest",
            bucket=settings.s3_bucket_public,
            key_or_prefix="centroid/index.json",
            is_exact_key=True,
        ),
        S3Check(
            name="common_snap_routes",
            bucket=settings.s3_bucket_public,
            key_or_prefix=f"{settings.s3_prefix_common}/snap-routes.json",
            is_exact_key=True,
        ),
        S3Check(
            name="common_school_list",
            bucket=settings.s3_bucket_public,
            key_or_prefix=f"{settings.s3_prefix_common}/school-list.json",
            is_exact_key=True,
        ),
        S3Check(
            name="assets_csv",
            bucket=settings.s3_bucket_dataproc,
            key_or_prefix=f"{settings.s3_prefix_assets}/{settings.asset_logo_csv_filename}",
            is_exact_key=True,
        ),
        S3Check(
            name="assets_manifest",
            bucket=settings.s3_bucket_public,
            key_or_prefix="manifest.json",
            is_exact_key=True,
        ),
        S3Check(
            name="sekolah_angkat_madani",
            bucket=settings.s3_bucket_dataproc,
            key_or_prefix=f"{settings.s3_prefix_sekolah_angkat_madani}/{settings.sekolah_angkat_madani_filename}",
            is_exact_key=True,
        ),
    ]


def evaluate_s3_bootstrap(settings: Settings | None = None) -> Tuple[List[str], List[CheckResult]]:
    """
    Run S3 readiness checks concurrently and return missing services.

    Returns:
        missing: list of service names below minimum object count
        results: list of CheckResult with counts for logging/diagnostics
    """
    if settings is None:
        settings = get_settings()

    checks = _build_checks(settings)
    if not checks:
        return [], []

    client = boto3.client("s3")
    missing: List[str] = []
    results: List[CheckResult] = []

    max_workers = min(8, len(checks)) or 1

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_map = {executor.submit(_count_objects, client, check): check for check in checks}
        for future in as_completed(future_map):
            check = future_map[future]
            try:
                count = future.result()
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "S3 check '%s' failed unexpectedly: %s", check.name, exc
                )
                count = 0

            results.append(CheckResult(name=check.name, found=count, required=check.min_objects))
            if count < check.min_objects:
                missing.append(check.name)

    results.sort(key=lambda item: item.name)
    missing.sort()
    return missing, results
