from __future__ import annotations

import pytest

from src.utils.retry import with_retry


def test_with_retry_succeeds_on_third_attempt() -> None:
    calls: list[int] = []

    @with_retry(attempts=4, initial_wait=0.001, max_wait=0.005, retry_on=(ValueError,))
    def flaky() -> str:
        calls.append(1)
        if len(calls) < 3:
            raise ValueError("transient")
        return "ok"

    assert flaky() == "ok"
    assert len(calls) == 3


def test_with_retry_raises_after_max() -> None:
    calls: list[int] = []

    @with_retry(attempts=2, initial_wait=0.001, max_wait=0.005, retry_on=(ValueError,))
    def always_fails() -> None:
        calls.append(1)
        raise ValueError("nope")

    with pytest.raises(ValueError):
        always_fails()
    assert len(calls) == 2


def test_with_retry_does_not_retry_unmatched_exception() -> None:
    calls: list[int] = []

    @with_retry(attempts=4, initial_wait=0.001, max_wait=0.005, retry_on=(ValueError,))
    def wrong_exception() -> None:
        calls.append(1)
        raise KeyError("not retried")

    with pytest.raises(KeyError):
        wrong_exception()
    assert len(calls) == 1
