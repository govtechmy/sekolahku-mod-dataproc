from __future__ import annotations

import json
import logging
from concurrent.futures import ThreadPoolExecutor
from typing import Any

from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)


__all__ = [
    "normalise_segment",
    "dumps_document",
    "build_parlimen_path",
    "final_key_from_temp",
    "upload_to_s3",
    "move_object",
    "move_staged_objects",
]


def normalise_segment(value: Any, fallback: str) -> str:
    text = (str(value).strip() if value else fallback).strip()
    if not text:
        text = fallback
    return text.replace("/", "-").replace(" ", "_")


def dumps_document(document: dict[str, Any]) -> bytes:
    return json.dumps(document, default=str, ensure_ascii=False).encode("utf-8")


def build_parlimen_path(document: dict[str, Any]) -> tuple[str, str, str]:
    data = document.get("data") or {}
    pentadbiran = data.get("infoPentadbiran") or {}

    negeri = normalise_segment(pentadbiran.get("negeri"), "UNKNOWN_NEGERI")
    parlimen = normalise_segment(pentadbiran.get("parlimen"), "UNKNOWN_PARLIMEN")
    kod_sekolah = normalise_segment(document.get("kodSekolah"), "UNKNOWN_KOD")

    return negeri, parlimen, kod_sekolah


def final_key_from_temp(temp_key: str, temp_prefix: str) -> str:
    prefix = f"{temp_prefix}/"
    return temp_key[len(prefix) :] if temp_key.startswith(prefix) else temp_key


def upload_to_s3(s3_client, bucket: str, temp_key: str, payload: bytes, kod_sekolah: str) -> None:
    try:
        s3_client.put_object(
            Bucket=bucket,
            Key=temp_key,
            Body=payload,
            ContentType="application/json",
        )
    except ClientError:
        logger.exception("Failed to upload sekolah=%s to temp key=%s", kod_sekolah, temp_key)
        raise


def move_object(s3_client, bucket: str, temp_key: str, temp_prefix: str) -> str:
    final_key = final_key_from_temp(temp_key, temp_prefix)
    copy_source = {"Bucket": bucket, "Key": temp_key}
    logger.debug("Moving temp key=%s to final key=%s", temp_key, final_key)
    try:
        s3_client.copy_object(Bucket=bucket, CopySource=copy_source, Key=final_key)
        logger.debug("Deleting temp key=%s after move", temp_key)
        s3_client.delete_object(Bucket=bucket, Key=temp_key)
    except ClientError:
        logger.exception("Failed to move/delete object %s -> %s", temp_key, final_key)
        raise
    return final_key


def move_staged_objects(
    s3_client,
    bucket: str,
    temp_keys: list[str],
    *,
    temp_prefix: str,
    max_workers: int,
) -> list[str]:
    final_keys: list[str] = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(move_object, s3_client, bucket, key, temp_prefix): key for key in temp_keys
        }
        for future in futures:
            final_keys.append(future.result())
    return final_keys

