"""Object storage abstraction.

Supports `s3://` and `gs://` URIs and routes them to:
    - boto3 (real S3)         when EXECUTION_MODE=live and creds present
    - boto3 -> MinIO          when EXECUTION_MODE=mock (default)
    - google-cloud-storage    when EXECUTION_MODE=live for GCS
    - fake-gcs-server         when EXECUTION_MODE=mock for GCS

The same write/read functions are used by the ingestion, transform and quality
layers so that switching from mock to real cloud is a single env-var flip.
"""

from __future__ import annotations

import io
import os
from typing import Any
from urllib.parse import urlparse

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from src.utils.logger import get_logger

log = get_logger(__name__)


def _parse_uri(uri: str) -> tuple[str, str, str]:
    """Return (scheme, bucket, key)."""
    parsed = urlparse(uri)
    if parsed.scheme not in {"s3", "gs"}:
        raise ValueError(f"Unsupported URI scheme: {uri}")
    return parsed.scheme, parsed.netloc, parsed.path.lstrip("/")


# ----------------------------- S3 / MinIO -------------------------------------

def _s3_client() -> Any:
    import boto3
    from botocore.client import Config

    return boto3.client(
        "s3",
        endpoint_url=os.getenv("AWS_S3_ENDPOINT_URL") or None,
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
        config=Config(signature_version="s3v4"),
    )


# ----------------------------- GCS / fake-gcs --------------------------------

def _gcs_client() -> Any:
    from google.cloud import storage  # type: ignore[import-untyped]

    endpoint = os.getenv("GCS_ENDPOINT_URL")
    if endpoint:
        # Point at fake-gcs-server.
        os.environ.setdefault("STORAGE_EMULATOR_HOST", endpoint)
        return storage.Client.create_anonymous_client()
    return storage.Client(project=os.getenv("GCP_PROJECT_ID"))


# -------------------------------- public --------------------------------------

def write_parquet_to_path(df: pd.DataFrame, uri: str) -> None:
    """Write a DataFrame to s3:// or gs:// as Snappy-compressed Parquet."""
    table = pa.Table.from_pandas(df, preserve_index=False)
    buf = io.BytesIO()
    pq.write_table(table, buf, compression="snappy")
    buf.seek(0)
    scheme, bucket, key = _parse_uri(uri)
    if scheme == "s3":
        client = _s3_client()
        # ensure bucket exists in mock mode
        try:
            client.head_bucket(Bucket=bucket)
        except Exception:
            client.create_bucket(Bucket=bucket)
        client.upload_fileobj(buf, bucket, key)
    else:
        client = _gcs_client()
        gcs_bucket = client.bucket(bucket)
        if not gcs_bucket.exists():
            gcs_bucket = client.create_bucket(bucket)
        gcs_bucket.blob(key).upload_from_file(buf, content_type="application/octet-stream")
    log.info("object_written", uri=uri, bytes=buf.tell())


def read_parquet_from_path(uri: str) -> pd.DataFrame:
    scheme, bucket, key = _parse_uri(uri)
    buf = io.BytesIO()
    if scheme == "s3":
        _s3_client().download_fileobj(bucket, key, buf)
    else:
        _gcs_client().bucket(bucket).blob(key).download_to_file(buf)
    buf.seek(0)
    return pq.read_table(buf).to_pandas()


def list_objects(uri_prefix: str) -> list[str]:
    scheme, bucket, prefix = _parse_uri(uri_prefix)
    if scheme == "s3":
        paginator = _s3_client().get_paginator("list_objects_v2")
        return [
            f"s3://{bucket}/{obj['Key']}"
            for page in paginator.paginate(Bucket=bucket, Prefix=prefix)
            for obj in page.get("Contents", [])
        ]
    blobs = _gcs_client().bucket(bucket).list_blobs(prefix=prefix)
    return [f"gs://{bucket}/{b.name}" for b in blobs]
