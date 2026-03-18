{{
    config(
        materialized='table'
    )
}}

with base as (

    select * from {{ ref('fct_transactions') }}

),

summary as (

    select
        card_network,
        card_type,
        transaction_size,
        time_of_day,
        risk_level,
        product_cd,

        count(*)                                            as total_transactions,
        sum(i_fraud)                                        as total_fraud,
        {{ safe_divide('sum(i_fraud)', 'count(*)') }}       as fraud_rate,
        round(avg(transaction_amt_dollars), 2)              as avg_transaction_amt,
        round(sum(transaction_amt_dollars), 2)              as total_volume,
        round(max(transaction_amt_dollars), 2)              as max_transaction_amt,
        round(min(transaction_amt_dollars), 2)              as min_transaction_amt

    from base
    group by 1, 2, 3, 4, 5, 6

)

select * from summary