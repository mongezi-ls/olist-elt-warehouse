{{
    config(
        materialized='table',
        tags=['marts', 'dimensions']
    )
}}

with products as (
    select * from {{ ref('stg_products') }}
),

categories as (
    select * from {{ ref('stg_product_category_translation') }}
),

joined as (
    select
        p.product_id,
        p.product_category_name,
        coalesce(c.category_name_en, p.product_category_name, 'unknown') as product_category_name_en,
        p.product_name_length,
        p.product_description_length,
        p.product_photos_count,
        p.product_weight_grams,
        p.product_length_cm,
        p.product_height_cm,
        p.product_width_cm
    from products p
    left join categories c
        on p.product_category_name = c.category_name_pt
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['product_id']) }} as product_key,
        product_id,
        product_category_name,
        product_category_name_en,
        product_name_length,
        product_description_length,
        product_photos_count,
        product_weight_grams,
        product_length_cm,
        product_height_cm,
        product_width_cm
    from joined
)

select * from final