{{
  config(
    materialized='view'
  )
}}

with source as (

    select * from {{ source('stripe', 'payments') }}

),

renamed as (

    select
        id as payment_id,
        order_id,
        payment_method,

-- Convert from cents to dollars
        amount / 100.0 as amount,

        current_timestamp() as _loaded_at

    from source

)

select * from renamed
