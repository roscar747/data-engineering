"""Public REST API extractor.

Designed for unauthenticated, rate-limited public endpoints. Demonstrates:
    - retry with exponential backoff and jitter
    - per-iteration parameter substitution (e.g. one call per coin)
    - in-memory rate limiting
    - Parquet land in Bronze with audit columns

This module powers `coingecko_market_chart` in pipeline_config.yaml.
"""

from __future__ import annotations

import time
from collections import deque
from datetime import datetime, timezone
from typing import Any

import pandas as pd
import requests

from src.ingestion.cloud_storage_extractor import write_parquet_to_path
from src.utils.config_loader import SourceConfig, app_settings
from src.utils.logger import correlation_context, get_logger
from src.utils.retry import with_retry

log = get_logger(__name__)


class _RateLimiter:
    """Simple sliding-window rate limiter (no external deps)."""

    def __init__(self, requests_per_minute: int) -> None:
        self._rpm = requests_per_minute
        self._calls: deque[float] = deque()

    def acquire(self) -> None:
        now = time.monotonic()
        # purge old
        while self._calls and now - self._calls[0] > 60:
            self._calls.popleft()
        if len(self._calls) >= self._rpm:
            sleep_for = 60 - (now - self._calls[0]) + 0.05
            log.info("rate_limit_sleep", seconds=round(sleep_for, 2))
            time.sleep(max(sleep_for, 0))
        self._calls.append(time.monotonic())


class ApiExtractor:
    """Generic REST API extractor driven by SourceConfig."""

    def __init__(self, source: SourceConfig) -> None:
        self.source = source
        self.cfg = source.extra
        rl = self.cfg.get("rate_limit", {}).get("requests_per_minute", 30)
        self.limiter = _RateLimiter(rl)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "FinSight/0.1 (+https://github.com/finsight)",
            "Accept": "application/json",
        })

    # ---------------------------------------------------------------- public
    def run(self) -> None:
        with correlation_context(source=self.source.name):
            log.info("api_ingest_start", source=self.source.name)
            iterate_cfg = self.cfg.get("iterate")
            if iterate_cfg:
                for value in iterate_cfg["values"]:
                    self._extract_one({iterate_cfg["param"]: value})
            else:
                self._extract_one({})
            log.info("api_ingest_complete", source=self.source.name)

    # --------------------------------------------------------------- private
    def _extract_one(self, iter_params: dict[str, str]) -> None:
        url, params = self._build_url(iter_params)
        payload = self._fetch(url, params)
        df = self._normalise(payload, iter_params)
        if df.empty:
            log.warning("api_empty_response", source=self.source.name, params=iter_params)
            return
        self._land(df, iter_params)

    def _build_url(self, iter_params: dict[str, str]) -> tuple[str, dict[str, Any]]:
        base = self.cfg["base_url"].rstrip("/")
        endpoint = self.cfg["endpoint"]
        for k, v in iter_params.items():
            endpoint = endpoint.replace("{" + k + "}", str(v))
        return f"{base}{endpoint}", dict(self.cfg.get("params", {}))

    @with_retry(
        attempts=5,
        retry_on=(requests.ConnectionError, requests.Timeout, requests.HTTPError),
    )
    def _fetch(self, url: str, params: dict[str, Any]) -> dict[str, Any]:
        self.limiter.acquire()
        log.info("http_request", url=url, params=params)
        resp = self.session.request(
            method=self.cfg.get("method", "GET"),
            url=url,
            params=params,
            timeout=30,
        )
        if resp.status_code == 429:
            # Force a back-off; tenacity will retry
            time.sleep(5)
            resp.raise_for_status()
        resp.raise_for_status()
        return resp.json()

    def _normalise(self, payload: dict[str, Any], iter_params: dict[str, str]) -> pd.DataFrame:
        """CoinGecko returns parallel arrays - flatten to a tidy frame.

        For other endpoints, override or extend.
        """
        if not payload:
            return pd.DataFrame()
        if {"prices", "total_volumes", "market_caps"} <= payload.keys():
            prices = pd.DataFrame(payload["prices"], columns=["snapshot_ts_ms", "price_usd"])
            volumes = pd.DataFrame(payload["total_volumes"], columns=["snapshot_ts_ms", "volume_usd"])
            mcaps = pd.DataFrame(payload["market_caps"], columns=["snapshot_ts_ms", "market_cap_usd"])
            df = prices.merge(volumes, on="snapshot_ts_ms").merge(mcaps, on="snapshot_ts_ms")
            df["snapshot_ts"] = pd.to_datetime(df["snapshot_ts_ms"], unit="ms", utc=True)
            df["snapshot_date"] = df["snapshot_ts"].dt.date
            df["coin_id"] = iter_params.get("coin_id")
        else:
            df = pd.json_normalize(payload)

        # audit columns - present at every layer
        df["_ingestion_ts"] = datetime.now(timezone.utc)
        df["_source_system"] = self.source.name
        df["_execution_mode"] = app_settings().EXECUTION_MODE
        return df

    def _land(self, df: pd.DataFrame, iter_params: dict[str, str]) -> None:
        path = self.source.bronze_path.rstrip("/") + "/"
        suffix = "_".join(f"{k}={v}" for k, v in iter_params.items())
        ingestion_date = datetime.now(timezone.utc).date().isoformat()
        target = (
            f"{path}ingestion_date={ingestion_date}/"
            f"part-{suffix or 'full'}.parquet"
        )
        write_parquet_to_path(df, target)
        log.info("bronze_landed", target=target, rows=len(df))
