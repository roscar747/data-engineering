{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['customer_key', 'transaction_date'],
    schema='gold',
    tags=['daily', 'finance', 'fct']
) }}

with txn as (

    select * from {{ ref('stg_transactions') }}

),

agg as (

    select
        {{ dbt_utils.generate_surrogate_key(['customer_id']) }}      as customer_key,
        customer_id,
        transaction_date,
        count(*)                                                     as txn_count,
        sum(amount_usd)                                              as gross_usd,
        sum(case when is_fraud then amount_usd else 0 end)           as fraud_usd,
        avg(amount_usd)                                              as avg_ticket_usd,
        count(distinct merchant_id)                                  as distinct_merchants,
        count(distinct merchant_category)                            as distinct_categories,
        max(transaction_ts)                                          as last_transaction_ts,
        current_timestamp                                            as _gold_ts
    from txn
    group by 1, 2, 3

)

select * from agg

{% if is_incremental() %}
where transaction_date >= (select coalesce(max(transaction_date), date '1900-01-01') from {{ this }})
{% endif %}
