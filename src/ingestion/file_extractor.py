"""File extractor for local files and downloadable URLs.

Streams large CSVs in chunks via PyArrow to avoid OOM on the ingestion node.
"""

from __future__ import annotations

from datetime import datetime, timezone
from io import BytesIO
from pathlib import Path

import pandas as pd
import requests

from src.ingestion.cloud_storage_extractor import write_parquet_to_path
from src.utils.config_loader import SourceConfig
from src.utils.logger import correlation_context, get_logger
from src.utils.retry import with_retry

log = get_logger(__name__)


class FileExtractor:
    def __init__(self, source: SourceConfig) -> None:
        self.source = source
        self.cfg = source.extra

    def run(self) -> None:
        with correlation_context(source=self.source.name):
            log.info("file_ingest_start", source=self.source.name, kind=self.source.kind)
            if self.source.kind == "file_local":
                self._extract_local()
            elif self.source.kind == "file_url":
                self._extract_url()
            else:
                raise ValueError(f"Unsupported kind: {self.source.kind}")

    # ------------------------------------------------------------------ local
    def _extract_local(self) -> None:
        path = Path(self.cfg["path"])
        if not path.exists():
            raise FileNotFoundError(f"Source file missing: {path}")
        # stream-read for large files
        chunk_iter = pd.read_csv(path, chunksize=100_000)
        ingestion_date = datetime.now(timezone.utc).date().isoformat()
        for i, chunk in enumerate(chunk_iter):
            chunk = self._add_audit(chunk)
            target = (
                self.source.bronze_path.rstrip("/")
                + f"/ingestion_date={ingestion_date}/part-{i:04d}.parquet"
            )
            write_parquet_to_path(chunk, target)
            log.info("local_chunk_landed", chunk=i, rows=len(chunk), target=target)

    # -------------------------------------------------------------------- url
    def _extract_url(self) -> None:
        ingestion_date = datetime.now(timezone.utc).date().isoformat()
        for series in self.cfg.get("series", []):
            params = {"id": series["id"]}
            df = self._download(self.cfg["base_url"], params)
            df["series_id"] = series["id"]
            df["series_description"] = series.get("description")
            df = self._add_audit(df)
            target = (
                self.source.bronze_path.rstrip("/")
                + f"/ingestion_date={ingestion_date}/series_id={series['id']}.parquet"
            )
            write_parquet_to_path(df, target)
            log.info("url_landed", series=series["id"], rows=len(df), target=target)

    @with_retry(attempts=4, retry_on=(requests.ConnectionError, requests.Timeout, requests.HTTPError))
    def _download(self, url: str, params: dict) -> pd.DataFrame:
        log.info("http_download", url=url, params=params)
        resp = requests.get(url, params=params, timeout=60)
        resp.raise_for_status()
        return pd.read_csv(BytesIO(resp.content))

    # ----------------------------------------------------------------- common
    def _add_audit(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        df["_ingestion_ts"] = datetime.now(timezone.utc)
        df["_source_system"] = self.source.name
        return df
