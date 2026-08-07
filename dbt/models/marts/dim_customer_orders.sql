{{ config(
    materialized='table',
    table_type='PRIMARY',
    key_type='PRIMARY',
    keys=['customer_id'],
    distributed_by=['customer_id'],
    buckets=3,
    properties={'replication_num': '1'}
) }}

with order_summary as (
    select
        customer_id,
        count(*) as order_count,
        sum(amount) as lifetime_value,
        max(order_id) as latest_order_id
    from {{ ref('stg_orders') }}
    group by customer_id
)

select
    customers.customer_id,
    customers.email,
    customers.full_name,
    customers.customer_status,
    coalesce(order_summary.order_count, 0) as order_count,
    coalesce(order_summary.lifetime_value, 0) as lifetime_value,
    order_summary.latest_order_id
from {{ ref('stg_customers') }} as customers
left join order_summary
    on customers.customer_id = order_summary.customer_id
