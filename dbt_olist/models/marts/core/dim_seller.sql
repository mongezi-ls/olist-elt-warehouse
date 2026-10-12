{{
    config(
        materialized='table',
        tags=['marts', 'dimensions']
    )
}}

with sellers as (
    select * from {{ ref('stg_sellers') }}
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['seller_id']) }} as seller_key,
        seller_id,
        seller_zip_code,
        seller_city,
        seller_state
    from sellers
)

select * from final