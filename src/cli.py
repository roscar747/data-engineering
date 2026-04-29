"""FinSight command-line interface.

Lightweight click-based wrapper that lets you invoke any pipeline stage
manually. Useful for local debugging and in CI smoke tests.
"""

from __future__ import annotations

import click

from src.utils.logger import configure_logging, get_logger

configure_logging()
log = get_logger(__name__)


@click.group()
def cli() -> None:
    """FinSight CLI."""


@cli.command("ingest")
@click.option("--source", required=True, help="Source name from pipeline_config.yaml")
def ingest(source: str) -> None:
    from src.ingestion.api_extractor import ApiExtractor
    from src.ingestion.file_extractor import FileExtractor
    from src.utils.config_loader import get_source

    src_cfg = get_source(source)
    log.info("ingest_start", source=src_cfg.name, kind=src_cfg.kind)
    if src_cfg.kind == "api":
        ApiExtractor(src_cfg).run()
    elif src_cfg.kind in ("file_local", "file_url"):
        FileExtractor(src_cfg).run()
    else:
        raise click.ClickException(f"Unknown source kind: {src_cfg.kind}")


@cli.command("transform")
@click.option("--job", required=True, help="Silver job name")
def transform(job: str) -> None:
    from src.transform.bronze_to_silver import BronzeToSilver
    BronzeToSilver(job_name=job).run()


@cli.command("load")
@click.option("--target", type=click.Choice(["snowflake", "bigquery", "mssql", "fabric"]))
@click.option("--mart", required=True)
def load(target: str, mart: str) -> None:
    from src.loaders import LOADERS
    LOADERS[target]().load(mart)


@cli.command("dq")
@click.option("--suite", default="all")
def dq(suite: str) -> None:
    from src.quality.data_quality_checks import run_suite
    run_suite(suite)


@cli.command("run-pipeline")
@click.option("--dag", default="master_finsight_dag")
def run_pipeline(dag: str) -> None:
    """Trigger a DAG via the Airflow REST API."""
    import os

    import requests

    base = os.getenv("AIRFLOW_API_URL", "http://localhost:8080/api/v1")
    auth = (os.getenv("AIRFLOW_USER", "admin"), os.getenv("AIRFLOW_PASSWORD", "admin"))
    response = requests.post(
        f"{base}/dags/{dag}/dagRuns",
        json={"conf": {}},
        auth=auth,
        timeout=30,
    )
    response.raise_for_status()
    log.info("dag_triggered", dag=dag, run_id=response.json().get("dag_run_id"))


if __name__ == "__main__":
    cli()
