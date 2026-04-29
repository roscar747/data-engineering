"""Standalone DAG: macro indicators (FRED) only."""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from src.ingestion.file_extractor import FileExtractor
from src.transform.bronze_to_silver import BronzeToSilver
from src.utils.config_loader import get_source

with DAG(
    dag_id="economic_indicators_dag",
    description="Daily FRED macro-economic indicators ingest",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["finsight", "macro"],
    default_args={
        "owner": "data-platform",
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
    },
) as dag:

    ingest = PythonOperator(
        task_id="ingest_fred",
        python_callable=lambda: FileExtractor(get_source("fred_macro_indicators")).run(),
    )

    transform = PythonOperator(
        task_id="silver_macro_indicators_clean",
        python_callable=lambda: BronzeToSilver("macro_indicators_clean").run(),
    )

    ingest >> transform
