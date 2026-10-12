{{
    config(
        materialized='table',
        tags=['marts', 'dimensions']
    )
}}

with customers as (
    select * from {{ ref('stg_customers') }}
),

-- One row per customer_unique_id (the actual person; customer_id is per-order)
deduped as (
    select
        customer_unique_id,
        customer_zip_code,
        customer_city,
        customer_state,
        row_number() over (
            partition by customer_unique_id
            order by customer_id
        ) as rn
    from customers
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['customer_unique_id']) }} as customer_key,
        customer_unique_id                                              as customer_id,
        customer_zip_code,
        customer_city,
        customer_state
    from deduped
    where rn = 1
)

select * from final