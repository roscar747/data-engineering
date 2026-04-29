"""Reusable retry decorator with exponential backoff and jitter.

Wraps tenacity to standardise behaviour across the codebase.
"""

from __future__ import annotations

from collections.abc import Callable
from functools import wraps
from typing import Any

from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
    wait_random,
)

from src.utils.logger import get_logger

log = get_logger(__name__)


def with_retry(
    *,
    attempts: int = 5,
    initial_wait: float = 2.0,
    max_wait: float = 60.0,
    backoff_factor: float = 2.0,
    retry_on: tuple[type[BaseException], ...] = (Exception,),
) -> Callable:
    """Standard FinSight retry policy.

    Example:
        @with_retry(attempts=3, retry_on=(requests.HTTPError,))
        def fetch():
            ...
    """

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        @retry(
            stop=stop_after_attempt(attempts),
            wait=wait_exponential(
                multiplier=initial_wait,
                max=max_wait,
                exp_base=backoff_factor,
            )
            + wait_random(0, 1),
            retry=retry_if_exception_type(retry_on),
            reraise=True,
        )
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            try:
                return func(*args, **kwargs)
            except retry_on as exc:
                log.warning(
                    "retryable_error",
                    func=func.__name__,
                    error_type=type(exc).__name__,
                    error=str(exc),
                )
                raise

        return wrapper

    return decorator
