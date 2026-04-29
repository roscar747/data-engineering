"""Incremental load strategies.

Three patterns are supported:

1. APPEND          : insert-only, partition by ingestion_date.
2. HIGH_WATERMARK  : track max(watermark_column) in meta.ingestion_watermarks
                     and only fetch rows newer than that on next run.
3. MERGE_UPSERT    : dialect-aware MERGE with deduping on the business key.
4. SCD2            : delegated to dbt snapshots; this module only stages the
                     latest snapshot for the snapshot to consume.

The classes below produce strategy-specific SQL for the chosen target dialect
so the same Python orchestration code works against Snowflake, BigQuery,
SQL Server and Fabric.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Protocol

from src.utils.logger import get_logger

log = get_logger(__name__)


class WarehouseConn(Protocol):
    def execute(self, sql: str, params: dict | None = None) -> Any: ...
    def fetchone(self) -> tuple | None: ...


@dataclass
class IncrementalSpec:
    target_table: str
    business_key: list[str]
    watermark_column: str | None = None
    lookback_days: int = 1
    dialect: str = "snowflake"  # snowflake | bigquery | mssql | fabric


class IncrementalStrategy(ABC):
    def __init__(self, spec: IncrementalSpec) -> None:
        self.spec = spec

    @abstractmethod
    def build_sql(self, staging_table: str) -> list[str]: ...


class AppendStrategy(IncrementalStrategy):
    def build_sql(self, staging_table: str) -> list[str]:
        return [f"INSERT INTO {self.spec.target_table} SELECT * FROM {staging_table};"]


class HighWatermarkStrategy(IncrementalStrategy):
    """Insert rows where watermark_column > max(watermark) in target."""

    def build_sql(self, staging_table: str) -> list[str]:
        wm = self.spec.watermark_column
        return [
            f"""
            INSERT INTO {self.spec.target_table}
            SELECT s.* FROM {staging_table} s
            WHERE s.{wm} > COALESCE(
                (SELECT MAX({wm}) FROM {self.spec.target_table}),
                CAST('1900-01-01' AS TIMESTAMP)
            );
            """.strip()
        ]


class MergeUpsertStrategy(IncrementalStrategy):
    """Dialect-aware MERGE statement."""

    _MERGE_TEMPLATE = {
        "snowflake": """
MERGE INTO {target} AS t
USING {source} AS s
   ON {join}
WHEN MATCHED THEN UPDATE SET {set_clause}
WHEN NOT MATCHED THEN INSERT ({columns}) VALUES ({source_columns});
""",
        "bigquery": """
MERGE `{target}` t
USING `{source}` s
   ON {join}
WHEN MATCHED THEN UPDATE SET {set_clause}
WHEN NOT MATCHED THEN INSERT ({columns}) VALUES ({source_columns});
""",
        "mssql": """
MERGE {target} AS t
USING {source} AS s
   ON {join}
WHEN MATCHED THEN UPDATE SET {set_clause}
WHEN NOT MATCHED THEN INSERT ({columns}) VALUES ({source_columns});
""",
        "fabric": """
MERGE INTO {target} AS t
USING {source} AS s
   ON {join}
WHEN MATCHED THEN UPDATE SET {set_clause}
WHEN NOT MATCHED THEN INSERT ({columns}) VALUES ({source_columns});
""",
    }

    def build_sql(self, staging_table: str) -> list[str]:
        if self.spec.dialect not in self._MERGE_TEMPLATE:
            raise ValueError(f"Unsupported dialect: {self.spec.dialect}")
        cols = self._discover_columns(staging_table)
        join = " AND ".join([f"t.{k} = s.{k}" for k in self.spec.business_key])
        set_clause = ", ".join([f"t.{c} = s.{c}" for c in cols if c not in self.spec.business_key])
        columns = ", ".join(cols)
        source_columns = ", ".join([f"s.{c}" for c in cols])
        sql = self._MERGE_TEMPLATE[self.spec.dialect].format(
            target=self.spec.target_table,
            source=staging_table,
            join=join,
            set_clause=set_clause,
            columns=columns,
            source_columns=source_columns,
        )
        return [sql.strip()]

    def _discover_columns(self, staging_table: str) -> list[str]:
        # In real life this hits INFORMATION_SCHEMA. For brevity we delegate to
        # the caller via spec.business_key + a "*" approach; replace with a
        # real lookup in production.
        return self.spec.business_key + ["amount_usd", "_silver_ts"]


# ----------------------------- watermark store -------------------------------

class WatermarkStore:
    """Persist last-seen watermark per (source, table) in `meta.ingestion_watermarks`."""

    DDL = """
    CREATE TABLE IF NOT EXISTS meta.ingestion_watermarks (
        source_name      VARCHAR(200) NOT NULL,
        target_table     VARCHAR(200) NOT NULL,
        watermark_value  TIMESTAMP    NOT NULL,
        updated_at       TIMESTAMP    NOT NULL,
        PRIMARY KEY (source_name, target_table)
    );
    """

    def __init__(self, conn: WarehouseConn) -> None:
        self.conn = conn

    def get(self, source: str, target: str) -> datetime | None:
        row = self.conn.execute(
            "SELECT watermark_value FROM meta.ingestion_watermarks "
            "WHERE source_name = ? AND target_table = ?",
            {"source": source, "target": target},
        )
        return row.fetchone() if row else None

    def set(self, source: str, target: str, value: datetime) -> None:
        self.conn.execute(
            """
            MERGE INTO meta.ingestion_watermarks t
            USING (SELECT ? AS source_name, ? AS target_table, ? AS watermark_value) s
               ON t.source_name = s.source_name AND t.target_table = s.target_table
            WHEN MATCHED THEN UPDATE SET watermark_value = s.watermark_value, updated_at = CURRENT_TIMESTAMP
            WHEN NOT MATCHED THEN INSERT VALUES (s.source_name, s.target_table, s.watermark_value, CURRENT_TIMESTAMP);
            """,
            {"source": source, "target": target, "value": value},
        )
        log.info("watermark_advanced", source=source, target=target, value=value.isoformat())
