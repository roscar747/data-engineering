"""Microsoft Fabric Lakehouse loader.

Strategy: write Delta-format Parquet to the Lakehouse OneLake path, then
trigger a Fabric Pipeline (or notebook) to register the table. In live mode
this uses the OneLake API via azure-storage-file-datalake; in mock mode we
write Delta files locally for inspection.
"""

from __future__ import annotations

import os
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from src.loaders.base import WarehouseLoader
from src.utils.logger import get_logger
from src.utils.secrets_manager import require_secret

log = get_logger(__name__)


class FabricLoader(WarehouseLoader):
    name = "fabric"

    def _onelake_url(self) -> str:
        ws = require_secret("FABRIC_WORKSPACE_ID")
        lh = require_secret("FABRIC_LAKEHOUSE_ID")
        return f"https://onelake.dfs.fabric.microsoft.com/{ws}/{lh}/Tables"

    def _write_live(self, df: pd.DataFrame, mart: str) -> None:
        from azure.identity import ClientSecretCredential
        from azure.storage.filedatalake import DataLakeServiceClient

        creds = ClientSecretCredential(
            tenant_id=require_secret("FABRIC_TENANT_ID"),
            client_id=require_secret("FABRIC_CLIENT_ID"),
            client_secret=require_secret("FABRIC_CLIENT_SECRET"),
        )
        service = DataLakeServiceClient(
            account_url="https://onelake.dfs.fabric.microsoft.com",
            credential=creds,
        )
        ws = require_secret("FABRIC_WORKSPACE_ID")
        lh = require_secret("FABRIC_LAKEHOUSE_ID")
        fs = service.get_file_system_client(ws)

        # Write a single parquet file. A real implementation would use
        # delta-rs or pyspark to produce a Delta table; we keep the example
        # focused and rely on Fabric notebooks/pipelines to register it.
        table = pa.Table.from_pandas(df, preserve_index=False)
        local_tmp = Path(os.getenv("TMPDIR", "/tmp")) / f"{mart}.parquet"
        pq.write_table(table, local_tmp, compression="snappy")
        remote = f"{lh}/Tables/{mart}/part-{datetime.now(timezone.utc).isoformat()}.parquet"
        with local_tmp.open("rb") as fh:
            fs.create_file(remote).upload_data(fh.read(), overwrite=True)
        log.info("fabric_lakehouse_write", remote=remote, rows=len(df))
