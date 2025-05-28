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
        o.order_status,
        coalesce(op.order_amount, 0) as order_amount
    from orders o
    left join order_payments op on o.order_id = op.order_id

),

-- Calculate customer-level metrics
customer_order_summary as (

    select
        customer_id,

        -- Order metrics
        count(*) as total_orders,
        count(case when order_status = 'complete' then 1 end) as completed_orders,

        -- Financial metrics
        sum(order_amount) as lifetime_value,
        avg(order_amount) as average_order_value,

        -- Timing metrics
        min(order_date) as first_order_date,
        max(order_date) as last_order_date,

        -- Calculate days between first and last order
        date_diff(max(order_date), min(order_date), day) as customer_lifetime_days

    from order_summary
    where order_amount > 0  -- Only include orders with successful payments
    group by customer_id

)

select * from customer_order_summary

