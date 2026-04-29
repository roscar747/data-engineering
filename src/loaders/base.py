"""Loader base class - common contract across the four warehouses."""

from __future__ import annotations

import os
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any

import pandas as pd

from src.ingestion.cloud_storage_extractor import list_objects, read_parquet_from_path
from src.utils.config_loader import get_pipeline_config
from src.utils.logger import correlation_context, get_logger

log = get_logger(__name__)


class WarehouseLoader(ABC):
    """Abstract base. Subclasses implement only the bits that differ per warehouse."""

    name: str = "base"

    def __init__(self) -> None:
        self.execution_mode = os.getenv("EXECUTION_MODE", "mock")

    # ---------- public --------------------------------------------------------
    def load(self, mart: str) -> None:
        with correlation_context(loader=self.name, mart=mart):
            log.info("loader_start", target=self.name, mart=mart, mode=self.execution_mode)
            df = self._read_silver_for_mart(mart)
            if df.empty:
                log.warning("loader_no_rows", mart=mart)
                return
            if self.execution_mode == "mock":
                self._write_local_mock(df, mart)
            else:
                self._write_live(df, mart)
            log.info("loader_complete", target=self.name, mart=mart, rows=len(df))

    # ---------- helpers shared across loaders ---------------------------------
    def _read_silver_for_mart(self, mart: str) -> pd.DataFrame:
        """Load the silver dataset matching this mart by convention.

        Convention: gold mart `fct_daily_market_metrics` is built from
        silver `crypto_prices_daily`, etc. In production this is parameterised
        in pipeline_config.yaml; here we use a thin lookup table.
        """
        mapping = {
            "fct_daily_market_metrics": "coingecko_market_chart",
            "dim_asset": "reference_assets",
            "fct_customer_transaction_summary": "synthetic_transactions",
        }
        source_name = mapping.get(mart)
        if source_name is None:
            raise KeyError(f"No silver source mapped for mart {mart}")

        source = next(s for s in get_pipeline_config()["sources"] if s["name"] == source_name)
        silver_uri = source["bronze_path"].replace("/bronze", "/silver").replace(
            "finsight-bronze", "finsight-silver"
        )
        files = list_objects(silver_uri)
        frames = [read_parquet_from_path(f) for f in files]
        return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

    def _write_local_mock(self, df: pd.DataFrame, mart: str) -> None:
        """Persist to a local DuckDB file that stands in for the live warehouse."""
        import duckdb

        target_dir = Path(__file__).resolve().parents[2] / "data" / "_mock_warehouses"
        target_dir.mkdir(parents=True, exist_ok=True)
        path = target_dir / f"{self.name}.duckdb"
        log.info("mock_warehouse_write", path=str(path), mart=mart)
        with duckdb.connect(str(path)) as conn:
            conn.register("df", df)
            conn.execute(f"CREATE SCHEMA IF NOT EXISTS gold;")
            conn.execute(f"CREATE OR REPLACE TABLE gold.{mart} AS SELECT * FROM df;")

    # ---------- subclass override -------------------------------------------
    @abstractmethod
    def _write_live(self, df: pd.DataFrame, mart: str) -> None: ...

    # ---------- handy connection helper for live mode -----------------------
    def _connection(self) -> Any:  # pragma: no cover - subclass concern
        raise NotImplementedError
