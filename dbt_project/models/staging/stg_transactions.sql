with source as (

    select * from {{ source('fintech_raw', 'raw_transactions') }}

),

cleaned as (

    select
        -- ids
        transaction_id,

        -- transaction details
        transaction_dt,
        transaction_amt,
        product_cd,

        -- card info
        card1,
        card2,
        card3,
        card4,
        card5,
        card6,

        -- address
        addr1,
        addr2,

        -- distance
        dist1,
        dist2,

        -- email domains
        p_emaildomain,
        r_emaildomain,

        -- aggregated features
        c1, c2, c3, c4, c5, c6, c7,
        c8, c9, c10, c11, c12, c13, c14,

        -- time delta features
        d1, d2, d3, d4, d5, d6, d7,
        d8, d9, d10, d11, d12, d13, d14, d15,

        -- match features
        m1, m2, m3, m4, m5, m6, m7, m8, m9,

        -- fraud label
        i_fraud,

        -- audit columns
        _loaded_at,
        current_timestamp() as _dbt_loaded_at

    from source
    where transaction_id is not null

)

select * from cleaned