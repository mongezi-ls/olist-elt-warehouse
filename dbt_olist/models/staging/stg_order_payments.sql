{{
    config(
        materialized='view',
        tags=['staging', 'order_payments']
    )
}}

with source as (
    select * from {{ source('bronze', 'order_payments') }}
),

renamed as (
    select
        order_id,
        payment_sequential,
        payment_type,
        payment_installments,
        cast(payment_value as decimal(18, 2)) as payment_amount
    from source
)

select * from renamed