{% snapshot dim_asset_snapshot %}

{{
    config(
        target_schema='snapshots',
        unique_key='asset_symbol',
        strategy='check',
        check_cols=['asset_name', 'asset_class', 'sector']
    )
}}

select
    asset_symbol,
    asset_name,
    asset_class,
    sector
from {{ source('silver', 'reference_assets_current') }}

{% endsnapshot %}
