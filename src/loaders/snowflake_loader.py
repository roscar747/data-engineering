"""Snowflake loader (live mode).

Uses snowflake-connector-python's `write_pandas` for bulk PUT+COPY behaviour.
"""

from __future__ import annotations

import pandas as pd

from src.loaders.base import WarehouseLoader
from src.utils.logger import get_logger
from src.utils.secrets_manager import require_secret

log = get_logger(__name__)


class SnowflakeLoader(WarehouseLoader):
    name = "snowflake"

    def _connection(self):
        import snowflake.connector
        return snowflake.connector.connect(
            account=require_secret("SNOWFLAKE_ACCOUNT"),
            user=require_secret("SNOWFLAKE_USER"),
            password=require_secret("SNOWFLAKE_PASSWORD"),
            role=require_secret("SNOWFLAKE_ROLE"),
            warehouse=require_secret("SNOWFLAKE_WAREHOUSE"),
            database=require_secret("SNOWFLAKE_DATABASE"),
            schema=require_secret("SNOWFLAKE_SCHEMA"),
        )

    def _write_live(self, df: pd.DataFrame, mart: str) -> None:
        from snowflake.connector.pandas_tools import write_pandas

        df.columns = [c.upper() for c in df.columns]
        with self._connection() as conn:
            success, n_chunks, n_rows, _ = write_pandas(
                conn=conn,
                df=df,
                table_name=mart.upper(),
                schema="GOLD",
                auto_create_table=True,
                overwrite=False,
            )
            log.info(
                "snowflake_write_pandas",
                success=success,
                chunks=n_chunks,
                rows=n_rows,
                table=mart.upper(),
            )
