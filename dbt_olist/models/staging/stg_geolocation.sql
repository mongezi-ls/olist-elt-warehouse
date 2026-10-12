{{
    config(
        materialized='view',
        tags=['staging', 'geolocation']
    )
}}

with source as (
    select * from {{ source('bronze', 'geolocation') }}
),

renamed as (
    select
        geolocation_zip_code_prefix as zip_code,
        cast(geolocation_lat as double) as latitude,
        cast(geolocation_lng as double) as longitude,
        geolocation_city as city,
        geolocation_state as state
    from source
)

select * from renamed