"""Silver -> Gold: thin wrapper around dbt.

The actual transformation logic for Gold lives in /dbt as proper dbt models so
the warehouse engine does the work. This module is responsible only for
invoking dbt with the right target, capturing exit codes, and surfacing
structured logs back to Airflow.
"""

from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

from src.utils.logger import correlation_context, get_logger

log = get_logger(__name__)

DBT_DIR = Path(__file__).resolve().parents[2] / "dbt"


def run_dbt(
    *,
    target: str,
    select: str | None = None,
    full_refresh: bool = False,
) -> None:
    """Invoke `dbt run` (or `dbt build` if select is None)."""
    if shutil.which("dbt") is None:
        raise RuntimeError("dbt not found on PATH - run `make install` first")

    cmd = ["dbt", "build" if select is None else "run", "--target", target]
    if select:
        cmd += ["--select", select]
    if full_refresh:
        cmd.append("--full-refresh")

    with correlation_context(dbt_target=target, dbt_select=select or "all"):
        log.info("dbt_invoke", cmd=" ".join(cmd))
        env = os.environ.copy()
        env["DBT_PROFILES_DIR"] = str(DBT_DIR)
        proc = subprocess.run(cmd, cwd=DBT_DIR, env=env, check=False)
        if proc.returncode != 0:
            raise RuntimeError(f"dbt failed with exit code {proc.returncode}")
        log.info("dbt_complete")
