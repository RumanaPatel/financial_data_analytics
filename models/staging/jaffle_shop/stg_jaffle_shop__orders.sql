{{
    config(
        materialized='view'
    )
}}

with source as (

    select * from {{ source('jaffle_shop', 'orders') }}

),

renamed as (

    select
        id as order_id,
        user_id as customer_id,
        order_date,
        status as order_status,
        current_timestamp() as _loaded_at

    from source

)

select * from renamed