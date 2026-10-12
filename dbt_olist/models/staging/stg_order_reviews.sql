{{
    config(
        materialized='view',
        tags=['staging', 'order_reviews']
    )
}}

with source as (
    select * from {{ source('bronze', 'order_reviews') }}
),

deduplicated as (
    select
        review_id,
        order_id,
        review_score,
        review_comment_title,
        review_comment_message,
        cast(review_creation_date as timestamp)    as reviewed_at,
        cast(review_answer_timestamp as timestamp) as answered_at,
        row_number() over (
            partition by review_id
            order by review_answer_timestamp desc
        ) as rn
    from source
),

final as (
    select
        review_id,
        order_id,
        review_score,
        review_comment_title,
        review_comment_message,
        reviewed_at,
        answered_at
    from deduplicated
    where rn = 1
)

select * from final