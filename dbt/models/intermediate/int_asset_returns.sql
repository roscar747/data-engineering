{{ config(materialized='ephemeral') }}

with prices as (

    select
        asset_symbol,
        trade_date,
        close_usd,
        volume_usd,
        market_cap_usd,
        lag(close_usd) over (
            partition by asset_symbol order by trade_date
        ) as prev_close
    from {{ ref('stg_crypto_prices') }}

),

returns as (

    select
        asset_symbol,
        trade_date,
        close_usd,
        volume_usd,
        market_cap_usd,
        case when prev_close is null or prev_close = 0 then null
             else (close_usd - prev_close) / prev_close * 100.0
        end as daily_return_pct
    from prices

),

with_volatility as (

    select
        *,
        stddev(daily_return_pct) over (
            partition by asset_symbol
            order by trade_date
            rows between 29 preceding and current row
        ) as rolling_30d_volatility,
        avg(daily_return_pct) over (
            partition by asset_symbol
            order by trade_date
            rows between 29 preceding and current row
        ) as rolling_30d_avg_return
    from returns

)

select * from with_volatility
