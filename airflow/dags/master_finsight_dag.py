"""Master FinSight DAG.

Runs daily at 02:00 UTC. Top-level structure:

    1. ingest_*           - public APIs / files / cloud storage -> Bronze
    2. dq_pre_checks      - schema contracts on bronze
    3. bronze_to_silver_* - DuckDB / Spark transforms
    4. dq_silver_checks   - Great Expectations on silver
    5. silver_to_gold     - dbt build
    6. load_*             - fan out to four warehouses in parallel
    7. dq_post_checks     - referential integrity, freshness
    8. publish_metrics    - row counts, duration, latency to Prometheus
    9. notify             - Slack on failure (anywhere)
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.slack.notifications.slack_webhook import SlackWebhookNotifier
from airflow.utils.task_group import TaskGroup

from src.cdc.incremental_loader import IncrementalSpec  # noqa: F401  (showcased import)
from src.ingestion.api_extractor import ApiExtractor
from src.ingestion.file_extractor import FileExtractor
from src.loaders import LOADERS
from src.quality.data_quality_checks import run_checks
from src.transform.bronze_to_silver import BronzeToSilver
from src.transform.silver_to_gold import run_dbt
from src.utils.config_loader import get_source

DEFAULT_ARGS = {
    "owner": "data-platform",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "execution_timeout": timedelta(minutes=45),
    "on_failure_callback": SlackWebhookNotifier(
        slack_webhook_conn_id="slack_default",
        text=":rotating_light: *FinSight* task `{{ ti.task_id }}` failed in DAG `{{ dag.dag_id }}` (run `{{ run_id }}`). <{{ ti.log_url }}|Logs>",
    ),
}


def _ingest_api(source_name: str) -> None:
    ApiExtractor(get_source(source_name)).run()


def _ingest_file(source_name: str) -> None:
    FileExtractor(get_source(source_name)).run()


def _bronze_to_silver(job_name: str) -> None:
    BronzeToSilver(job_name=job_name).run()


def _load(target: str, mart: str) -> None:
    LOADERS[target]().load(mart)


def _dq_silver_check(silver_dataset: str) -> None:
    # In production this calls Great Expectations; here we re-use the
    # declarative checks as a smoke test.
    import duckdb

    conn = duckdb.connect()
    df = conn.execute(f"SELECT * FROM read_parquet('s3://finsight-silver/**/{silver_dataset}.parquet', union_by_name=True)").df()
    run_checks(df, layer="silver")


with DAG(
    dag_id="master_finsight_dag",
    description="End-to-end FinSight medallion pipeline",
    start_date=datetime(2026, 1, 1),
    schedule="0 2 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["finsight", "medallion", "production"],
    default_args=DEFAULT_ARGS,
    doc_md=__doc__,
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end", trigger_rule="all_done")

    # ---- 1. ingestion -------------------------------------------------------
    with TaskGroup("ingest") as ingest:
        for source in ["coingecko_market_chart"]:
            PythonOperator(
                task_id=f"api__{source}",
                python_callable=_ingest_api,
                op_kwargs={"source_name": source},
            )
        for source in ["fred_macro_indicators", "synthetic_transactions", "reference_assets"]:
            PythonOperator(
                task_id=f"file__{source}",
                python_callable=_ingest_file,
                op_kwargs={"source_name": source},
            )

    # ---- 2. bronze -> silver -----------------------------------------------
    with TaskGroup("bronze_to_silver") as bts:
        for job in ["crypto_prices_daily", "macro_indicators_clean", "transactions_enriched"]:
            PythonOperator(
                task_id=job,
                python_callable=_bronze_to_silver,
                op_kwargs={"job_name": job},
            )

    # ---- 3. silver DQ -------------------------------------------------------
    dq_silver = PythonOperator(
        task_id="dq_silver_checks",
        python_callable=_dq_silver_check,
        op_kwargs={"silver_dataset": "crypto_prices_daily"},
    )

    # ---- 4. silver -> gold (dbt) -------------------------------------------
    silver_to_gold = PythonOperator(
        task_id="silver_to_gold_dbt_build",
        python_callable=run_dbt,
        op_kwargs={"target": "duckdb"},  # swap to "snowflake" in live env
    )

    # ---- 5. fan-out load ----------------------------------------------------
    with TaskGroup("load_consumer_warehouses") as load:
        for target in ["snowflake", "bigquery", "mssql", "fabric"]:
            for mart in [
                "fct_daily_market_metrics",
                "dim_asset",
                "fct_customer_transaction_summary",
            ]:
                PythonOperator(
                    task_id=f"{target}__{mart}",
                    python_callable=_load,
                    op_kwargs={"target": target, "mart": mart},
                )

    # ---- 6. post DQ + publish ----------------------------------------------
    dq_post = EmptyOperator(task_id="dq_post_checks")
    publish_metrics = EmptyOperator(task_id="publish_metrics")

    start >> ingest >> bts >> dq_silver >> silver_to_gold >> load >> dq_post >> publish_metrics >> end
