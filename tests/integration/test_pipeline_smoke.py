"""Integration smoke test.

Runs against the local docker-compose stack. Skipped automatically if MinIO
is unreachable, so it can't fail CI on developer machines without the stack.
"""

from __future__ import annotations

import os

import pytest
import requests

pytestmark = pytest.mark.integration


def _minio_up() -> bool:
    endpoint = os.getenv("AWS_S3_ENDPOINT_URL", "http://localhost:9000")
    try:
        return requests.get(f"{endpoint}/minio/health/live", timeout=2).status_code == 200
    except Exception:
        return False


@pytest.mark.skipif(not _minio_up(), reason="local stack not running")
def test_seed_creates_buckets() -> None:
    from src.utils.seed_local_infra import _s3_client, ensure_bucket

    s3 = _s3_client()
    ensure_bucket(s3, "finsight-bronze")
    ensure_bucket(s3, "finsight-silver")
    response = s3.list_buckets()
    names = {b["Name"] for b in response["Buckets"]}
    assert "finsight-bronze" in names
    assert "finsight-silver" in names
