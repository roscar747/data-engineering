"""Warehouse loaders.

Each loader implements the same `WarehouseLoader` interface so the orchestrator
fan-out is symmetric. EXECUTION_MODE=mock swaps in a DuckDB-backed local
stand-in that provides the same `load(mart)` semantics so the pipeline can be
fully exercised offline.
"""

from __future__ import annotations

from src.loaders.base import WarehouseLoader
from src.loaders.bigquery_loader import BigQueryLoader
from src.loaders.fabric_loader import FabricLoader
from src.loaders.mssql_loader import MssqlLoader
from src.loaders.snowflake_loader import SnowflakeLoader

LOADERS: dict[str, type[WarehouseLoader]] = {
    "snowflake": SnowflakeLoader,
    "bigquery": BigQueryLoader,
    "mssql": MssqlLoader,
    "fabric": FabricLoader,
}

__all__ = ["LOADERS", "WarehouseLoader"]
