{{
    config(
        materialized='incremental',
        unique_key='transaction_id',
        on_schema_change='sync_all_columns'
    )
}}

with enriched as (

    select * from {{ ref('int_transactions_enriched') }}

),

final as (

    select
        transaction_id,
        transaction_dt,
        transaction_amt,
        product_cd,
        card_network,
        card_type,
        transaction_size,
        email_domain_match,
        time_of_day,
        p_emaildomain,
        r_emaildomain,
        i_fraud,
        _loaded_at,
        _dbt_loaded_at

    from enriched

    {% if is_incremental() %}
        where _loaded_at > (select max(_loaded_at) from {{ this }})
    {% endif %}

)

select * from final