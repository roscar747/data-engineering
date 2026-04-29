"""MS SQL Server loader.

Uses pyodbc + fast_executemany for high-throughput inserts.
A staging table is loaded in batches, then a MERGE statement upserts into the
target gold table.
"""

from __future__ import annotations

import pandas as pd

from src.loaders.base import WarehouseLoader
from src.utils.logger import get_logger
from src.utils.secrets_manager import require_secret

log = get_logger(__name__)


class MssqlLoader(WarehouseLoader):
    name = "mssql"

    def _connection_string(self) -> str:
        return (
            f"DRIVER={{{require_secret('MSSQL_DRIVER')}}};"
            f"SERVER={require_secret('MSSQL_HOST')},{require_secret('MSSQL_PORT')};"
            f"DATABASE={require_secret('MSSQL_DATABASE')};"
            f"UID={require_secret('MSSQL_USER')};"
            f"PWD={require_secret('MSSQL_PASSWORD')};"
            "TrustServerCertificate=yes;"
        )

    def _write_live(self, df: pd.DataFrame, mart: str) -> None:
        import pyodbc

        cols = list(df.columns)
        placeholders = ", ".join(["?"] * len(cols))
        col_list = ", ".join(cols)
        staging = f"##stg_{mart}"

        with pyodbc.connect(self._connection_string(), autocommit=False) as conn:
            cur = conn.cursor()
            cur.fast_executemany = True

            # 1. create temp staging table mirroring df schema
            cur.execute(self._create_table_sql(staging, df))

            # 2. bulk insert in batches
            batch_size = 5000
            rows = df.itertuples(index=False, name=None)
            buf: list[tuple] = []
            inserted = 0
            for row in rows:
                buf.append(row)
                if len(buf) >= batch_size:
                    cur.executemany(f"INSERT INTO {staging} ({col_list}) VALUES ({placeholders})", buf)
                    inserted += len(buf)
                    buf.clear()
            if buf:
                cur.executemany(f"INSERT INTO {staging} ({col_list}) VALUES ({placeholders})", buf)
                inserted += len(buf)

            # 3. MERGE into gold target
            target = f"gold.{mart}"
            cur.execute(self._merge_sql(target, staging, cols))
            conn.commit()
            log.info("mssql_merge_complete", target=target, rows_staged=inserted)

    # ------------------------------------------------------------------ helpers
    @staticmethod
    def _create_table_sql(table: str, df: pd.DataFrame) -> str:
        type_map = {
            "int64": "BIGINT",
            "Int64": "BIGINT",
            "float64": "FLOAT",
            "object": "NVARCHAR(MAX)",
            "bool": "BIT",
            "datetime64[ns]": "DATETIME2",
            "datetime64[ns, UTC]": "DATETIME2",
        }
        cols = ", ".join(
            f"[{c}] {type_map.get(str(df[c].dtype), 'NVARCHAR(MAX)')}"
            for c in df.columns
        )
        return f"CREATE TABLE {table} ({cols});"

    @staticmethod
    def _merge_sql(target: str, source: str, cols: list[str]) -> str:
        # Use the first column as the synthetic key when no business key declared.
        key = cols[0]
        set_clause = ", ".join([f"t.[{c}] = s.[{c}]" for c in cols if c != key])
        col_list = ", ".join([f"[{c}]" for c in cols])
        src_list = ", ".join([f"s.[{c}]" for c in cols])
        return f"""
        MERGE {target} AS t
        USING {source} AS s
           ON t.[{key}] = s.[{key}]
        WHEN MATCHED THEN UPDATE SET {set_clause}
        WHEN NOT MATCHED THEN INSERT ({col_list}) VALUES ({src_list});
        """
