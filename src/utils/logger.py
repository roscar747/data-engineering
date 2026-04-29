"""Structured logging with correlation IDs.

Use:
    from src.utils.logger import get_logger
    log = get_logger(__name__)
    log.info("ingest_started", source="coingecko", rows_expected=1000)

The logger produces JSON in production and human-friendly key=value lines in dev.
A correlation_id (default: Airflow run_id) is bound for the duration of a task.
"""

from __future__ import annotations

import logging
import os
import sys
import uuid
from contextlib import contextmanager
from typing import Any

import structlog

_BOUND_CORRELATION_ID: str | None = None


def configure_logging(level: str | None = None, json: bool | None = None) -> None:
    """Configure structlog + stdlib logging once per process."""
    level = (level or os.getenv("LOG_LEVEL", "INFO")).upper()
    use_json = json if json is not None else os.getenv("ENVIRONMENT", "local") != "local"

    timestamper = structlog.processors.TimeStamper(fmt="iso", utc=True)
    shared = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        timestamper,
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
    ]

    structlog.configure(
        processors=shared
        + [
            structlog.processors.JSONRenderer()
            if use_json
            else structlog.dev.ConsoleRenderer(colors=sys.stdout.isatty()),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(getattr(logging, level)),
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )
    logging.basicConfig(format="%(message)s", stream=sys.stdout, level=level)


def get_logger(name: str | None = None) -> structlog.stdlib.BoundLogger:
    if not structlog.is_configured():
        configure_logging()
    return structlog.get_logger(name or "finsight")


@contextmanager
def correlation_context(correlation_id: str | None = None, **bind: Any):
    """Bind a correlation_id (and any additional context) for the duration of a block."""
    cid = correlation_id or os.getenv("AIRFLOW_CTX_DAG_RUN_ID") or str(uuid.uuid4())
    token = structlog.contextvars.bind_contextvars(correlation_id=cid, **bind)
    try:
        yield cid
    finally:
        structlog.contextvars.reset_contextvars(**token) if isinstance(token, dict) else structlog.contextvars.clear_contextvars()
