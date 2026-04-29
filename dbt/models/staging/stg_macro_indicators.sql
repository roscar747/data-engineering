{{ config(materialized='view', schema='silver') }}

select
    series_id,
    series_description,
    observation_date,
    observation_value,
    _silver_ts as silver_ts
from {{ source('silver', 'macro_indicators_clean') }}
