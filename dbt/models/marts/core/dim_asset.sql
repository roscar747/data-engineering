{{ config(
    materialized='table',
    schema='gold',
    unique_key='asset_key',
    tags=['daily', 'core', 'dim']
) }}

-- SCD Type 2 dimension for assets.
-- Source: dbt snapshot snapshots/dim_asset_snapshot.sql

with snap as (

    select
        asset_symbol,
        asset_name,
        asset_class,
        sector,
        dbt_valid_from        as effective_from,
        dbt_valid_to          as effective_to,
        case when dbt_valid_to is null then 1 else 0 end as is_current,
        {{ dbt_utils.generate_surrogate_key(['asset_symbol', 'dbt_valid_from']) }} as asset_key
    from {{ ref('dim_asset_snapshot') }}

)

select * from snap
