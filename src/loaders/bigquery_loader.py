"""BigQuery loader (live mode)."""

from __future__ import annotations

import os

import pandas as pd

from src.loaders.base import WarehouseLoader
from src.utils.logger import get_logger
from src.utils.secrets_manager import require_secret

log = get_logger(__name__)


class BigQueryLoader(WarehouseLoader):
    name = "bigquery"

    def _client(self):
        from google.cloud import bigquery
        return bigquery.Client(
            project=require_secret("BQ_PROJECT_ID"),
            location=os.getenv("BQ_LOCATION", "US"),
        )

    def _write_live(self, df: pd.DataFrame, mart: str) -> None:
        from google.cloud import bigquery

        dataset = require_secret("BQ_DATASET_GOLD")
        client = self._client()
        table_id = f"{client.project}.{dataset}.{mart}"

        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION],
            time_partitioning=bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="trade_date" if "trade_date" in df.columns else None,
            ) if "trade_date" in df.columns else None,
            clustering_fields=[c for c in ("asset_symbol", "trade_date") if c in df.columns] or None,
        )
        job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()
        log.info("bigquery_load_job", table=table_id, rows=job.output_rows)
