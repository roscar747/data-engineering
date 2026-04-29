from __future__ import annotations

from src.cdc.incremental_loader import (
    AppendStrategy,
    HighWatermarkStrategy,
    IncrementalSpec,
    MergeUpsertStrategy,
)


def test_append_emits_simple_insert() -> None:
    spec = IncrementalSpec(target_table="gold.fct", business_key=["id"])
    sql = AppendStrategy(spec).build_sql("stg")
    assert sql == ["INSERT INTO gold.fct SELECT * FROM stg;"]


def test_high_watermark_filters_by_max_watermark() -> None:
    spec = IncrementalSpec(
        target_table="gold.fct",
        business_key=["id"],
        watermark_column="updated_at",
    )
    sql = HighWatermarkStrategy(spec).build_sql("stg")
    assert "MAX(updated_at)" in sql[0]
    assert "INSERT INTO gold.fct" in sql[0]


def test_merge_supports_all_dialects() -> None:
    for dialect in ("snowflake", "bigquery", "mssql", "fabric"):
        spec = IncrementalSpec(
            target_table="gold.fct_customer",
            business_key=["customer_key"],
            dialect=dialect,
        )
        sql = MergeUpsertStrategy(spec).build_sql("stg_customer")[0]
        assert "MERGE" in sql.upper()
        assert "WHEN MATCHED" in sql.upper()
        assert "WHEN NOT MATCHED" in sql.upper()
