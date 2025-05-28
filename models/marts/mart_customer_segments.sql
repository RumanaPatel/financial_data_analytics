{{ 
    config(
        materialized='table'
    ) 
}}

with customers as (

    select * from {{ ref('stg_jaffle_shop__customers') }}

),

customer_orders as (

    select * from {{ ref('int_jaffle_shop__customer_order_summary') }}

)

select
    c.customer_id,
    c.full_name,
    c.first_name,
    c.last_name,

    -- Order metrics
    coalesce(co.total_orders, 0) as total_orders,
    coalesce(co.completed_orders, 0) as completed_orders,
    coalesce(co.lifetime_value, 0) as lifetime_value,
    coalesce(co.average_order_value, 0) as average_order_value,

    -- Date fields
    co.first_order_date,
    co.last_order_date,
    coalesce(co.customer_lifetime_days, 0) as customer_lifetime_days,

    -- Days since last order
    case
        when co.last_order_date is null then null
        else date_diff(current_date(), co.last_order_date, day)
    end as days_since_last_order,

    -- Status segment
    case
        when co.last_order_date is null then 'never_ordered'
        when date_diff(current_date(), co.last_order_date, day) <= 90 then 'active'
        when date_diff(current_date(), co.last_order_date, day) <= 365 then 'dormant'
        else 'churned'
    end as customer_status,

    -- Value segment
    case
        when coalesce(co.lifetime_value, 0) = 0 then 'no_value'
        when co.lifetime_value < 50 then 'low_value'
        when co.lifetime_value < 200 then 'medium_value'
        else 'high_value'
    end as value_segment,

    -- Frequency segment
    case
        when coalesce(co.total_orders, 0) = 0 then 'no_orders'
        when co.total_orders = 1 then 'one_time'
        when co.total_orders <= 3 then 'occasional'
        else 'frequent'
    end as frequency_segment

from customers c
left join customer_orders co
    on c.customer_id = co.customer_id
