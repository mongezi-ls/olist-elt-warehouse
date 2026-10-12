{{
    config(
        materialized='view',
        tags=['staging', 'order_items']
    )
}}

with source as (
    select * from {{ source('bronze', 'order_items') }}
),

renamed as (
    select
        order_id,
        order_item_id,
        product_id,
        seller_id,
        cast(shipping_limit_date as timestamp) as shipped_by,
        cast(price as decimal(18, 2))          as item_price,
        cast(freight_value as decimal(18, 2))  as freight_amount
    from source
)

select * from renamed