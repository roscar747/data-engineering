"""Bronze -> Silver transformations powered by DuckDB.

DuckDB is chosen for the local/portfolio scenario because it gives Spark-like
SQL semantics, reads Parquet directly out of S3/GCS via httpfs, and runs in a
single Python process. The same SQL would translate verbatim to Spark SQL or
Trino in production - the engine is swappable via `engine: spark|duckdb` in
pipeline_config.yaml.
"""

from __future__ import annotations

import os
from datetime import datetime, timezone

import duckdb

from src.ingestion.cloud_storage_extractor import list_objects, write_parquet_to_path
from src.utils.config_loader import get_pipeline_config
from src.utils.logger import correlation_context, get_logger

log = get_logger(__name__)


# Map silver job name -> SQL to apply on top of unioned bronze.
_SILVER_SQL: dict[str, str] = {
    "crypto_prices_daily": """
        SELECT
            coin_id                                       AS asset_symbol,
            CAST(snapshot_date AS DATE)                   AS snapshot_date,
            CAST(snapshot_ts   AS TIMESTAMP)              AS snapshot_ts,
            AVG(price_usd)                                AS close_usd,
            AVG(volume_usd)                               AS volume_usd,
            AVG(market_cap_usd)                           AS market_cap_usd,
            COUNT(*)                                      AS source_row_count,
            CURRENT_TIMESTAMP                             AS _silver_ts
        FROM bronze
        GROUP BY 1,2,3
    """,
    "macro_indicators_clean": """
        SELECT
            UPPER(series_id)                              AS series_id,
            series_description,
            CAST(observation_date AS DATE)                AS observation_date,
            TRY_CAST(observation_value AS DOUBLE)         AS observation_value,
            CURRENT_TIMESTAMP                             AS _silver_ts
        FROM (
            SELECT
                series_id,
                series_description,
                COALESCE("DATE", observation_date)        AS observation_date,
                COALESCE(TRY_CAST(value AS VARCHAR),
                         TRY_CAST(observation_value AS VARCHAR)) AS observation_value
            FROM bronze
        )
        WHERE observation_value IS NOT NULL
    """,
    "transactions_enriched": """
        SELECT
            transaction_id,
            customer_id,
            CAST(transaction_date AS DATE)                AS transaction_date,
            CAST(transaction_ts AS TIMESTAMP)             AS transaction_ts,
            currency,
            amount,
            -- Mock FX: in production this would join a daily FX dim
            amount * CASE currency
                WHEN 'USD' THEN 1.00
                WHEN 'EUR' THEN 1.07
                WHEN 'GBP' THEN 1.27
                WHEN 'JPY' THEN 0.0064
                ELSE 1.00
            END                                           AS amount_usd,
            merchant_id,
            merchant_category,
            country,
            channel,
            CAST(is_fraud AS BOOLEAN)                     AS is_fraud,
            -- PII masking for downstream marts
            'sha256:' || substr(md5(email), 1, 16)        AS email_hashed,
            regexp_replace(ip_address, '\\d+$', 'xxx')    AS ip_address_masked,
            CURRENT_TIMESTAMP                             AS _silver_ts
        FROM bronze
    """,
}


class BronzeToSilver:
    def __init__(self, job_name: str) -> None:
        self.job_name = job_name
        self.cfg = self._lookup_job(job_name)
        self.source_cfg = self._lookup_source(self.cfg["source"])

    def run(self) -> None:
        with correlation_context(silver_job=self.job_name):
            log.info("silver_job_start", job=self.job_name)
            df = self._transform()
            self._land(df)
            log.info("silver_job_complete", job=self.job_name, rows=len(df))

    # ------------------------------------------------------------------ helpers
    def _lookup_job(self, name: str) -> dict:
        for j in get_pipeline_config().get("silver_jobs", []):
            if j["name"] == name:
                return j
        raise KeyError(f"Silver job '{name}' not found")

    def _lookup_source(self, name: str) -> dict:
        for s in get_pipeline_config().get("sources", []):
            if s["name"] == name:
                return s
        raise KeyError(f"Source '{name}' not found")

    def _transform(self):
        bronze_uri_prefix = self.source_cfg["bronze_path"]
        files = list_objects(bronze_uri_prefix)
        if not files:
            raise RuntimeError(f"No bronze files under {bronze_uri_prefix}")
        log.info("bronze_files_discovered", count=len(files))

        conn = duckdb.connect()
        # configure httpfs / s3 endpoint for MinIO
        conn.execute("INSTALL httpfs; LOAD httpfs;")
        endpoint = os.getenv("AWS_S3_ENDPOINT_URL", "").replace("http://", "").replace("https://", "")
        if endpoint:
            conn.execute(f"SET s3_endpoint='{endpoint}';")
            conn.execute("SET s3_url_style='path';")
            conn.execute("SET s3_use_ssl=false;")
        conn.execute(f"SET s3_access_key_id='{os.getenv('AWS_ACCESS_KEY_ID', '')}';")
        conn.execute(f"SET s3_secret_access_key='{os.getenv('AWS_SECRET_ACCESS_KEY', '')}';")

        # union-all read of bronze parquet
        glob = bronze_uri_prefix.rstrip("/") + "/**/*.parquet"
        conn.execute(f"CREATE OR REPLACE VIEW bronze AS SELECT * FROM read_parquet('{glob}', union_by_name=True);")

        sql = _SILVER_SQL.get(self.job_name)
        if sql is None:
            raise KeyError(f"No SQL template registered for silver job '{self.job_name}'")
        return conn.execute(sql).df()

    def _land(self, df) -> None:
        silver_root = self.source_cfg["bronze_path"].replace("/bronze", "/silver").replace(
            "finsight-bronze", "finsight-silver"
        )
        target = silver_root.rstrip("/") + f"/run_date={datetime.now(timezone.utc).date().isoformat()}/{self.job_name}.parquet"
        write_parquet_to_path(df, target)
