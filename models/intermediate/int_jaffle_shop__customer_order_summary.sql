{{
    config(
        materialized='view'
    )
}}

with payments as (

    select * from {{ ref("stg_jaffle_shop__payments") }}

),

orders as (

    select * from {{ ref('stg_jaffle_shop__orders') }}

),

order_payments as (

    select
        order_id,
        sum(amount) as order_amount
    from payments
    where amount > 0
    group by order_id

),

order_summary as (

    select
        o.order_id,
        o.customer_id,
        o.order_date,
        o.status,
        coalesce(op.order_amount, 0) as order_amount
    from orders o
    left join order_payments op on o.order_id = op.order_id

)

select * from order_summary