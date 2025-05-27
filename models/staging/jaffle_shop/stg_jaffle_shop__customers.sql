{{
    config(
        materialized='view'
    )
}}

with 

source as (

    select * from {{ source('jaffle_shop', 'customers') }}

),

renamed as (

    select
        id,
        first_name,
        last_name
        first_name || ' ' || last_name as full_name,

        --metadata for lineage
        current_timestamp() as _loaded_at

    from source

)

select * from renamed
