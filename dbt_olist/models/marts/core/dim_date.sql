{{
    config(
        materialized='table',
        tags=['marts', 'dimensions']
    )
}}

with date_spine as (
    {{
        dbt_utils.date_spine(
            datepart="day",
            start_date="cast('2016-01-01' as date)",
            end_date="cast('2020-12-31' as date)"
        )
    }}
),

final as (
    select
        cast(date_day as date)          as date_day,
        {{ dbt_utils.generate_surrogate_key(['date_day']) }} as date_key,

        -- day attributes
        dayofweek(date_day)                 as day_of_week_num,
        date_format(date_day, 'EEEE')       as day_of_week_name,
        extract(day from date_day)          as day_of_month,
        dayofyear(date_day)                 as day_of_year,

        -- week attributes
        weekofyear(date_day)                as week_of_year,

        -- month attributes
        extract(month from date_day)        as month_num,
        date_format(date_day, 'MMMM')       as month_name,
        date_format(date_day, 'yyyy-MM')    as year_month,

        -- quarter attributes
        extract(quarter from date_day)      as quarter_num,
        concat('Q', extract(quarter from date_day)) as quarter_name,

        -- year attributes
        extract(year from date_day)         as year_num,

        -- flags
        case when dayofweek(date_day) in (1, 7) then true else false end as is_weekend,
        case when date_day = last_day(date_day) then true else false end as is_month_end

    from date_spine
)

select * from final