select
    id as customer_id,
    email,
    full_name,
    status as customer_status
from {{ source('realtime', 'customers_current') }}
