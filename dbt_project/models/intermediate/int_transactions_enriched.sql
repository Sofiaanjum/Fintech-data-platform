with transactions as (

    select * from {{ ref('stg_transactions') }}

),

enriched as (

    select
        transaction_id,
        transaction_dt,
        transaction_amt,
        product_cd,
        card4,
        card6,
        p_emaildomain,
        r_emaildomain,
        i_fraud,

        -- transaction size bucket
        case
            when transaction_amt < 50    then 'small'
            when transaction_amt < 500   then 'medium'
            when transaction_amt < 5000  then 'large'
            else 'very_large'
        end as transaction_size,

        -- email domain match flag
        case
            when p_emaildomain = r_emaildomain then true
            else false
        end as email_domain_match,

        -- card network
        case
            when card4 = 'visa'       then 'Visa'
            when card4 = 'mastercard' then 'Mastercard'
            when card4 = 'discover'   then 'Discover'
            when card4 = 'amex'       then 'Amex'
            else 'Other'
        end as card_network,

        -- card type
        case
            when card6 = 'credit' then 'Credit'
            when card6 = 'debit'  then 'Debit'
            else 'Unknown'
        end as card_type,

        -- time of day bucket based on transaction_dt
        case
            when mod(transaction_dt, 86400) between 0     and 21600  then 'night'
            when mod(transaction_dt, 86400) between 21600 and 43200  then 'morning'
            when mod(transaction_dt, 86400) between 43200 and 64800  then 'afternoon'
            else 'evening'
        end as time_of_day,

        _loaded_at,
        _dbt_loaded_at

    from transactions

)

select * from enriched