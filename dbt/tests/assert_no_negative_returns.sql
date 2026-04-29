-- Singular test: rolling volatility must always be non-negative.
-- A negative value indicates a calculation error upstream.

select asset_symbol, trade_date, rolling_30d_volatility
from {{ ref('fct_daily_market_metrics') }}
where rolling_30d_volatility < 0
