{{
    config(
        materialized='view',
        tags=['staging', 'product_categories']
    )
}}

with source as (
    select * from {{ source('bronze', 'product_category_name_translation') }}
),

renamed as (
    select
        product_category_name         as category_name_pt,
        product_category_name_english as category_name_en
    from source
)

select * from renamed