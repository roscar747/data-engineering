# Architecture

## Why these choices?

### Medallion (Bronze / Silver / Gold)
Most regulated industries land "raw, immutable, source-faithful" data first
(Bronze) so that any future re-derivation is auditable. Silver is the layer
where contracts are enforced and PII is masked. Gold is shaped for the
business — star schemas, wide marts, well-tested.

### DuckDB + Spark dual stance
The repository runs locally on DuckDB so contributors can iterate without
spinning up a Spark cluster. The same SQL transports to Spark in production
(`engine: spark` in pipeline_config.yaml). The interfaces are deliberately
narrow so the engine can change without rewriting the pipeline.

### Why four warehouses?
Real platforms accumulate warehouses through M&A, departmental purchasing
decisions, and migration timelines. The point of FinSight is to demonstrate
how a single transformation tier can fan out to **multiple consumer warehouses
via the same Gold contract** — Snowflake (Finance), BigQuery (Marketing),
MS SQL (Risk on-prem), Fabric (Exec dashboards).

### dbt for Gold, Python for Bronze->Silver
- **Bronze->Silver** does heavy schema evolution, type casting, partition
  rewrites, parquet IO. Python/Spark/DuckDB excels here.
- **Silver->Gold** is set-based business logic and dbt's strengths (lineage,
  tests, docs, incremental strategies) are well-suited.

### Airflow over alternatives
Airflow remains the most widely adopted orchestrator and the one most senior
hiring managers expect to see. Prefect / Dagster patterns are documented in
`docs/runbook.md` for awareness, but not implemented here to avoid churn.

## Pipeline operating contract

| Concern        | Contract                                                  |
| -------------- | --------------------------------------------------------- |
| SLA            | Gold marts refreshed by 04:00 UTC daily                   |
| Freshness      | Silver no older than 36h, Gold no older than 24h          |
| Idempotency    | All loaders are upsert/MERGE; safe to re-run any task     |
| Backfill       | `--start-date` parameter accepted on every Airflow DAG    |
| Rollback       | Bronze immutable; rebuild Silver/Gold by re-running tasks |
| Observability  | structlog JSON, OpenLineage events, Prometheus metrics    |

## Failure modes & recovery

1. **API rate limit exceeded** — handled by tenacity backoff + sliding-window limiter; alerts only after 5 retries.
2. **Schema drift** — caught by Pandera at Bronze ingest; raises `DataQualityError` and stops the DAG before bad data propagates.
3. **Loader failure on one warehouse** — does NOT block the others; uses `trigger_rule='all_done'` on the post-DQ task and reports per-target results.
4. **Stale source** — freshness check catches; alert fires while the rest of the DAG continues with previous-day data.

## Security

- Secrets resolved via the `secrets_manager` abstraction (AWS / GCP / Azure / env). Code never reads sensitive values directly.
- PII columns hashed at Silver (`email`, `ip_address`).
- Roles in each warehouse limit the loader to the schemas it owns.
- CI scans every PR for committed secret patterns and dependency CVEs.
