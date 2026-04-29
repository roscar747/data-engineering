# Runbook

## Daily operations

### Healthy run signals
- All tasks in `master_finsight_dag` are green by 04:00 UTC.
- `meta.dq_results` shows zero `status='FAIL'` rows for the latest run_id.
- Slack `#data-platform-alerts` channel is quiet.

### Quick triage

| Symptom                                  | First check                                     |
| ---------------------------------------- | ----------------------------------------------- |
| DAG failed at `ingest__coingecko`        | Public endpoint or rate limit; check `http_request` log lines |
| `bronze_to_silver` task OOM              | Switch engine to `spark`; bump executor memory  |
| `dq_silver_checks` failed                | Inspect `_Check` failures in task logs          |
| One loader failed, others passed         | Re-run that single task, no full DAG restart    |
| All loaders failed                       | Check shared upstream (Silver) before warehouse |

### Manual ops

```bash
# Backfill a date range
airflow dags backfill master_finsight_dag --start-date 2024-12-01 --end-date 2024-12-07

# Re-run a single failed task
airflow tasks clear master_finsight_dag --task-regex 'load_consumer_warehouses\..*snowflake.*'

# Trigger the standalone crypto DAG only
airflow dags trigger crypto_pipeline_dag
```

## Cost / performance levers

| Lever                               | Where                                |
| ----------------------------------- | ------------------------------------ |
| Snowflake warehouse size            | `pipeline_config.yaml` -> warehouses |
| Auto-suspend                        | DDL: `01_database_and_schemas.sql`   |
| BigQuery clustering                 | `dbt_project.yml` model config       |
| Bronze partitioning                 | `defaults.bronze.partition_by`       |
| Incremental vs full-refresh         | `incremental` block per source       |
| dbt threads                         | `profiles.yml`                       |

## Disaster recovery

1. **Lost Silver data** -> rerun `bronze_to_silver` (Bronze is the immutable truth).
2. **Lost Gold data** -> rerun `silver_to_gold_dbt_build` and the loaders.
3. **Lost Bronze data** -> only recoverable from the source (acceptable trade-off; documented).
4. **Corrupt watermark** -> clear `meta.ingestion_watermarks` row and let the next run re-derive.

## Migrating to dbt Cloud / Dagster / Prefect

The orchestration layer is intentionally thin. Each Python task in
`master_finsight_dag.py` calls a stand-alone callable. Lifting these into
Dagster ops or Prefect flows is mechanical: replace the Airflow operator
boilerplate, keep the callables.
