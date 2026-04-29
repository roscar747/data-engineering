# Data Model

## Gold layer (consumer-facing)

### Star schema

```
                                +------------------+
                                |    dim_asset     |
                                | (SCD2)           |
                                |  asset_key  PK   |
                                |  asset_symbol    |
                                |  asset_class     |
                                |  effective_from  |
                                |  effective_to    |
                                +------------------+
                                          |
                                          | asset_key
                                          v
+----------------------+    asset_key     +-------------------------+
| dim_currency (seed)  |<-----------------|  fct_daily_market_      |
|  currency_code  PK   |                  |  metrics                |
+----------------------+                  |  asset_key, trade_date  |
                                          |  close_usd              |
                                          |  daily_return_pct       |
                                          |  rolling_30d_volatility |
                                          +-------------------------+

+----------------------+    customer_key
| dim_customer (impl)  |<-----------------+ fct_customer_transaction_summary
|  customer_key  PK    |                  |  customer_key, transaction_date
+----------------------+                  |  gross_usd, fraud_usd
                                          +-------------------------+
```

### Grain

| Mart                                | Grain                              |
| ----------------------------------- | ---------------------------------- |
| `dim_asset`                         | One row per asset version (SCD2)   |
| `fct_daily_market_metrics`          | One row per (asset_key, trade_date)|
| `fct_customer_transaction_summary`  | One row per (customer_key, date)   |

### SCD2 strategy

`dim_asset` is built off a dbt snapshot (`dim_asset_snapshot.sql`) with a
`check` strategy on `asset_name`, `asset_class`, `sector`. New rows are
versioned automatically; the `is_current = 1` flag is the standard
"point-in-time" filter.

## Silver layer (cleaned, conformed)

| Table                        | Notes                                   |
| ---------------------------- | --------------------------------------- |
| `crypto_prices_daily`        | Aggregated to one row per asset/day     |
| `macro_indicators_clean`     | Long-format (series_id, date, value)    |
| `transactions_enriched`      | FX-normalised, PII hashed/masked        |
| `reference_assets_current`   | Latest snapshot of reference asset master|

## Bronze layer (raw)

Bronze tables mirror the source byte-for-byte and are partitioned by
`ingestion_date` and `source_system`. They are append-only — no updates, no
deletes. The retention policy is 365 days enforced by an S3/GCS lifecycle
policy (out of scope for this repo but documented in `runbook.md`).
