{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['asset_key', 'trade_date'],
    cluster_by=['trade_date', 'asset_symbol'],
    schema='gold',
    tags=['daily', 'core', 'fct']
) }}

with returns as (

    select * from {{ ref('int_asset_returns') }}

),

dim as (

    select asset_key, asset_symbol, asset_class
    from {{ ref('dim_asset') }}
    where is_current = 1

),

joined as (

    select
        d.asset_key,
        r.asset_symbol,
        r.trade_date,
        r.close_usd,
        r.volume_usd,
        r.market_cap_usd,
        r.daily_return_pct,
        r.rolling_30d_volatility,
        r.rolling_30d_avg_return,
        d.asset_class,
        current_timestamp as _gold_ts
    from returns r
    join dim d using (asset_symbol)

)

select * from joined

{% if is_incremental() %}
where trade_date >= (select coalesce(max(trade_date), date '1900-01-01') from {{ this }})
{% endif %}
