{% snapshot snap_customers %}

{{
    config(
        target_schema='dev_snapshots',
        unique_key='customer_unique_id',
        strategy='check',
        check_cols=['customer_city', 'customer_state', 'customer_zip_code'],
        invalidate_hard_deletes=True
    )
}}

with source as (
    select * from {{ ref('stg_customers') }}
),

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
    from source
    where customer_unique_id is not null
),

final as (
    select
        customer_unique_id,
        customer_zip_code,
        customer_city,
        customer_state
    from deduped
    where rn = 1
)

select * from final

{% endsnapshot %}