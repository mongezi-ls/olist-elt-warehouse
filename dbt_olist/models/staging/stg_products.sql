{{
    config(
        materialized='view',
        tags=['staging', 'products']
    )
}}

with source as (
    select * from {{ source('bronze', 'products') }}
),

renamed as (
    select
        product_id,
        product_category_name,
        -- Fix typos from source data
        product_name_lenght        as product_name_length,
        product_description_lenght as product_description_length,
        product_photos_qty         as product_photos_count,
        product_weight_g           as product_weight_grams,
        product_length_cm          as product_length_cm,
        product_height_cm          as product_height_cm,
        product_width_cm           as product_width_cm
    from source
)

select * from renamed