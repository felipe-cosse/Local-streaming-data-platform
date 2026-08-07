select
    id as order_id,
    customer_id,
    status as order_status,
    amount
from {{ source('realtime', 'orders_current') }}
