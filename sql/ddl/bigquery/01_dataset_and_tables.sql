-- =============================================================================
-- BigQuery DDL - FinSight gold dataset
-- Run via `bq query --use_legacy_sql=false < this_file`.
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS `${BQ_PROJECT_ID}.finsight_gold`
OPTIONS (
  location = "${BQ_LOCATION}",
  description = "FinSight gold-layer marts"
);

CREATE OR REPLACE TABLE `${BQ_PROJECT_ID}.finsight_gold.dim_asset` (
    asset_key       STRING    NOT NULL OPTIONS(description="SCD2 surrogate"),
    asset_symbol    STRING    NOT NULL,
    asset_name      STRING,
    asset_class     STRING,
    sector          STRING,
    effective_from  TIMESTAMP NOT NULL,
    effective_to    TIMESTAMP,
    is_current      INT64     NOT NULL
)
PARTITION BY DATE(effective_from)
CLUSTER BY asset_symbol, effective_from;

CREATE OR REPLACE TABLE `${BQ_PROJECT_ID}.finsight_gold.fct_daily_market_metrics` (
    asset_key               STRING  NOT NULL,
    asset_symbol            STRING  NOT NULL,
    trade_date              DATE    NOT NULL,
    close_usd               NUMERIC,
    volume_usd              NUMERIC,
    market_cap_usd          NUMERIC,
    daily_return_pct        FLOAT64,
    rolling_30d_volatility  FLOAT64,
    rolling_30d_avg_return  FLOAT64,
    asset_class             STRING,
    _gold_ts                TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
PARTITION BY trade_date
CLUSTER BY asset_symbol, trade_date;

CREATE OR REPLACE TABLE `${BQ_PROJECT_ID}.finsight_gold.fct_customer_transaction_summary` (
    customer_key         STRING  NOT NULL,
    customer_id          STRING  NOT NULL,
    transaction_date     DATE    NOT NULL,
    txn_count            INT64,
    gross_usd            NUMERIC,
    fraud_usd            NUMERIC,
    avg_ticket_usd       NUMERIC,
    distinct_merchants   INT64,
    distinct_categories  INT64,
    last_transaction_ts  TIMESTAMP,
    _gold_ts             TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
PARTITION BY transaction_date
CLUSTER BY customer_id;
