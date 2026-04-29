"""Standalone DAG: crypto market data only.

Useful when triaging crypto-source-specific issues without touching the rest
of the pipeline. Reuses the same callables as the master DAG.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from src.ingestion.api_extractor import ApiExtractor
from src.transform.bronze_to_silver import BronzeToSilver
from src.utils.config_loader import get_source

with DAG(
    dag_id="crypto_pipeline_dag",
    description="Ad-hoc crypto-only pipeline",
    start_date=datetime(2026, 1, 1),
    schedule="0 4 * * *",
    catchup=False,
    tags=["finsight", "crypto"],
    default_args={
        "owner": "data-platform",
        "retries": 2,
        "retry_delay": timedelta(minutes=3),
    },
) as dag:

    ingest = PythonOperator(
        task_id="ingest_coingecko",
        python_callable=lambda: ApiExtractor(get_source("coingecko_market_chart")).run(),
    )

    transform = PythonOperator(
        task_id="silver_crypto_prices_daily",
        python_callable=lambda: BronzeToSilver("crypto_prices_daily").run(),
    )

    ingest >> transform
