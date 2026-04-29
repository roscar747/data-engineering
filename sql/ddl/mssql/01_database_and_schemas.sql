-- =============================================================================
-- MS SQL Server DDL - FinSight database, schemas, gold tables.
-- =============================================================================

IF DB_ID('FinSight') IS NULL
    CREATE DATABASE FinSight;
GO

USE FinSight;
GO

IF SCHEMA_ID('bronze') IS NULL EXEC('CREATE SCHEMA bronze');
IF SCHEMA_ID('silver') IS NULL EXEC('CREATE SCHEMA silver');
IF SCHEMA_ID('gold')   IS NULL EXEC('CREATE SCHEMA gold');
IF SCHEMA_ID('meta')   IS NULL EXEC('CREATE SCHEMA meta');
GO

-- ---------- Gold tables ------------------------------------------------------

IF OBJECT_ID('gold.dim_asset') IS NULL
CREATE TABLE gold.dim_asset (
    asset_key        VARCHAR(64)  NOT NULL,
    asset_symbol     VARCHAR(64)  NOT NULL,
    asset_name       VARCHAR(256),
    asset_class      VARCHAR(32),
    sector           VARCHAR(64),
    effective_from   DATETIME2     NOT NULL,
    effective_to     DATETIME2     NULL,
    is_current       BIT          NOT NULL,
    CONSTRAINT PK_dim_asset PRIMARY KEY CLUSTERED (asset_key)
);
GO

IF OBJECT_ID('gold.fct_daily_market_metrics') IS NULL
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
    _gold_ts                 DATETIME2 DEFAULT SYSUTCDATETIME(),
    CONSTRAINT PK_fct_daily_market PRIMARY KEY CLUSTERED (asset_key, trade_date)
);
CREATE NONCLUSTERED INDEX IX_fct_daily_market_symbol ON gold.fct_daily_market_metrics (asset_symbol, trade_date);
GO

IF OBJECT_ID('gold.fct_customer_transaction_summary') IS NULL
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
    _gold_ts             DATETIME2 DEFAULT SYSUTCDATETIME(),
    CONSTRAINT PK_fct_customer_txn PRIMARY KEY CLUSTERED (customer_key, transaction_date)
);
GO

-- ---------- Meta tables ------------------------------------------------------

IF OBJECT_ID('meta.ingestion_watermarks') IS NULL
CREATE TABLE meta.ingestion_watermarks (
    source_name     VARCHAR(200) NOT NULL,
    target_table    VARCHAR(200) NOT NULL,
    watermark_value DATETIME2     NOT NULL,
    updated_at      DATETIME2     NOT NULL,
    CONSTRAINT PK_watermarks PRIMARY KEY (source_name, target_table)
);
GO
