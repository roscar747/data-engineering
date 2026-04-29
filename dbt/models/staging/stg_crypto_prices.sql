{{ config(materialized='view', schema='silver') }}

with src as (

    select
        asset_symbol,
        snapshot_date     as trade_date,
        close_usd,
        volume_usd,
        market_cap_usd,
        _silver_ts        as silver_ts
    from {{ source('silver', 'crypto_prices_daily') }}

)

select * from src
