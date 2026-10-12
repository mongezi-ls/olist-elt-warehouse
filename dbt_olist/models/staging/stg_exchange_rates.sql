{{
    config(
        materialized='view',
        tags=['staging', 'exchange_rates']
    )
}}

with source as (
    select * from {{ source('bronze', 'exchange_rates_raw') }}
),

deduplicated as (
    select
        base_currency,
        cast(rate_date as date)       as rate_date,
        target_currency,
        cast(rate as double)          as exchange_rate,
        cast(ingested_at as timestamp) as ingested_at,
        row_number() over (
            partition by base_currency, rate_date, target_currency
            order by ingested_at desc
        ) as rn
    from source
),

final as (
    select
        base_currency,
        rate_date,
        target_currency,
        exchange_rate,
        ingested_at
    from deduplicated
    where rn = 1
)

select * from final