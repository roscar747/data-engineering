{{ config(materialized='view', schema='silver') }}

select
    transaction_id,
    customer_id,
    transaction_date,
    transaction_ts,
    currency,
    amount,
    amount_usd,
    merchant_id,
    merchant_category,
    country,
    channel,
    is_fraud,
    email_hashed,
    ip_address_masked,
    _silver_ts as silver_ts
from {{ source('silver', 'transactions_enriched') }}
