-- =============================================================================
-- Snowflake DDL - FinSight target schemas
-- Run as a role with USAGE on the target warehouse and CREATE DATABASE perms.
-- =============================================================================

CREATE DATABASE IF NOT EXISTS FINSIGHT;

USE DATABASE FINSIGHT;

CREATE SCHEMA IF NOT EXISTS BRONZE  COMMENT = 'Raw immutable from sources';
CREATE SCHEMA IF NOT EXISTS SILVER  COMMENT = 'Cleaned and conformed';
CREATE SCHEMA IF NOT EXISTS GOLD    COMMENT = 'Business-ready facts and dims';
CREATE SCHEMA IF NOT EXISTS META    COMMENT = 'Watermarks and DQ logs';
CREATE SCHEMA IF NOT EXISTS SNAPSHOTS COMMENT = 'dbt SCD2 snapshots';

-- ---------- Resource hygiene -------------------------------------------------

CREATE WAREHOUSE IF NOT EXISTS FINSIGHT_WH_XS
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Used for ingestion and small marts';

CREATE WAREHOUSE IF NOT EXISTS FINSIGHT_WH_S
    WAREHOUSE_SIZE = 'SMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Used for gold mart materialisation and ad-hoc analytics';

-- ---------- Roles ------------------------------------------------------------

CREATE ROLE IF NOT EXISTS FINSIGHT_LOADER;
GRANT USAGE     ON WAREHOUSE FINSIGHT_WH_XS TO ROLE FINSIGHT_LOADER;
GRANT USAGE     ON DATABASE  FINSIGHT       TO ROLE FINSIGHT_LOADER;
GRANT ALL       ON SCHEMA FINSIGHT.BRONZE   TO ROLE FINSIGHT_LOADER;
GRANT ALL       ON SCHEMA FINSIGHT.SILVER   TO ROLE FINSIGHT_LOADER;
GRANT ALL       ON SCHEMA FINSIGHT.META     TO ROLE FINSIGHT_LOADER;
