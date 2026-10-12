{{
    config(
        materialized='table',
        tags=['marts', 'facts']
    )
}}

with orders as (
    select * from {{ ref('stg_orders') }}
),

order_items as (
    select * from {{ ref('stg_order_items') }}
),

order_payments as (
    select * from {{ ref('stg_order_payments') }}
),

order_reviews as (
    select * from {{ ref('stg_order_reviews') }}
),

-- Aggregate payments to order level (an order may have multiple payment records)
payments_agg as (
    select
        order_id,
        sum(payment_amount) as total_payment_amount,
        count(*)            as payment_count
    from order_payments
    group by order_id
),

-- Reviews are at order level (already deduped to 1 row per review_id)
reviews_agg as (
    select
        order_id,
        avg(review_score) as avg_review_score,
        min(review_score) as min_review_score,
        max(review_score) as max_review_score
    from order_reviews
    group by order_id
),

-- Join order_items (grain) with orders + aggregations
joined as (
    select
        oi.order_id,
        oi.order_item_id,
        oi.product_id,
        oi.seller_id,
        o.customer_id,

        -- Dates
        cast(o.ordered_at as date) as order_date,
        o.ordered_at,
        o.approved_at,
        o.delivered_to_carrier_at,
        o.delivered_to_customer_at,
        o.estimated_delivery_at,

        -- Status
        o.order_status,

        -- Measures from items
        oi.item_price,
        oi.freight_amount,
        oi.item_price + oi.freight_amount as item_total,

        -- Measures from payments
        coalesce(p.total_payment_amount, 0) as total_payment_amount,
        coalesce(p.payment_count, 0)        as payment_count,

        -- Measures from reviews
        r.avg_review_score,
        r.min_review_score,
        r.max_review_score,

        -- Derived flags
        case when o.order_status = 'delivered' then true else false end as is_delivered,
        case when o.order_status = 'canceled'  then true else false end as is_canceled

    from order_items oi
    inner join orders o
        on oi.order_id = o.order_id
    left join payments_agg p
        on oi.order_id = p.order_id
    left join reviews_agg r
        on oi.order_id = r.order_id
),

final as (
    select
        -- Surrogate key for the fact row
        {{ dbt_utils.generate_surrogate_key(['order_id', 'order_item_id']) }} as order_item_key,

        -- Foreign keys to dimensions
        {{ dbt_utils.generate_surrogate_key(['order_date']) }}              as date_key,
        {{ dbt_utils.generate_surrogate_key(['customer_id']) }}             as customer_key,
        {{ dbt_utils.generate_surrogate_key(['product_id']) }}              as product_key,
        {{ dbt_utils.generate_surrogate_key(['seller_id']) }}               as seller_key,

        -- Natural keys for debugging
        order_id,
        order_item_id,

        -- Facts and attributes
        order_status,
        order_date,
        ordered_at,
        approved_at,
        delivered_to_carrier_at,
        delivered_to_customer_at,
        estimated_delivery_at,
        item_price,
        freight_amount,
        item_total,
        total_payment_amount,
        payment_count,
        avg_review_score,
        min_review_score,
        max_review_score,
        is_delivered,
        is_canceled

    from joined
)

select * from final