from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd
import pytest

from src.quality.data_quality_checks import (
    DataQualityError,
    SCHEMAS,
    _Check,
    validate_schema,
)


def test_not_null_passes() -> None:
    df = pd.DataFrame({"a": [1, 2, 3]})
    assert _Check.not_null(df, ["a"]) == []


def test_not_null_reports_failures() -> None:
    df = pd.DataFrame({"a": [1, None, 3]})
    failures = _Check.not_null(df, ["a"])
    assert any("nulls" in f for f in failures)


def test_unique_detects_duplicates() -> None:
    df = pd.DataFrame({"k": [1, 1, 2]})
    failures = _Check.unique(df, ["k"])
    assert failures and "duplicates" in failures[0]


def test_freshness_threshold() -> None:
    df = pd.DataFrame({"_ingestion_ts": [datetime(2000, 1, 1, tzinfo=timezone.utc)]})
    failures = _Check.freshness_hours(df, threshold=24)
    assert failures and "stale" in failures[0]


def test_schema_validate_synthetic_transactions() -> None:
    df = pd.DataFrame(
        {
            "transaction_id":   ["T1"],
            "customer_id":      ["C1"],
            "transaction_date": ["2024-01-01"],
            "amount":           [10.0],
            "currency":         ["USD"],
            "is_fraud":         [0],
        }
    )
    # Should not raise.
    validate_schema(df, "synthetic_transactions")


def test_schema_validate_rejects_invalid_currency() -> None:
    df = pd.DataFrame(
        {
            "transaction_id":   ["T1"],
            "customer_id":      ["C1"],
            "transaction_date": ["2024-01-01"],
            "amount":           [10.0],
            "currency":         ["XXX"],  # invalid
            "is_fraud":         [0],
        }
    )
    with pytest.raises(DataQualityError):
        validate_schema(df, "synthetic_transactions")


def test_all_registered_schemas_have_required_fields() -> None:
    for name, schema in SCHEMAS.items():
        assert schema.columns, f"Schema {name} has no columns"
