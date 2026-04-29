-- =============================================================================
-- Microsoft Fabric Warehouse DDL.
-- Fabric T-SQL is largely compatible with SQL Server DDL with a few caveats:
--   - no IDENTITY on warehouse, no DEFAULT on insert from external pipelines.
--   - schemas must already exist (created via the Fabric UI/API).
-- =============================================================================

CREATE SCHEMA gold;
GO
CREATE SCHEMA meta;
GO

CREATE TABLE gold.dim_asset (
    asset_key        VARCHAR(64)  NOT NULL,
    asset_symbol     VARCHAR(64)  NOT NULL,
    asset_name       VARCHAR(256),
    asset_class      VARCHAR(32),
    sector           VARCHAR(64),
    effective_from   DATETIME2     NOT NULL,
    effective_to     DATETIME2,
    is_current       BIT           NOT NULL
);
GO

CREATE TABLE gold.fct_daily_market_metrics (
    asset_key                VARCHAR(64) NOT NULL,
    asset_symbol             VARCHAR(64) NOT NULL,
    trade_date               DATE        NOT NULL,
    close_usd                DECIMAL(28,8),
    volume_usd               DECIMAL(28,4),
    market_cap_usd           DECIMAL(28,4),
    daily_return_pct         FLOAT,
    rolling_30d_volatility   FLOAT,
    rolling_30d_avg_return   FLOAT,
    asset_class              VARCHAR(32),
    _gold_ts                 DATETIME2
);
GO

CREATE TABLE gold.fct_customer_transaction_summary (
    customer_key         VARCHAR(64) NOT NULL,
    customer_id          VARCHAR(64) NOT NULL,
    transaction_date     DATE        NOT NULL,
    txn_count            BIGINT,
    gross_usd            DECIMAL(28,4),
    fraud_usd            DECIMAL(28,4),
    avg_ticket_usd       DECIMAL(28,4),
    distinct_merchants   BIGINT,
    distinct_categories  BIGINT,
    last_transaction_ts  DATETIME2,
    _gold_ts             DATETIME2
);
GO

CREATE TABLE meta.ingestion_watermarks (
    source_name      VARCHAR(200) NOT NULL,
    target_table     VARCHAR(200) NOT NULL,
    watermark_value  DATETIME2     NOT NULL,
    updated_at       DATETIME2     NOT NULL
);
GO
