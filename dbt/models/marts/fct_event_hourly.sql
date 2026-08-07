{{ config(
    materialized='table',
    table_type='DUPLICATE',
    distributed_by=['event_hour'],
    buckets=3,
    properties={'replication_num': '1'}
) }}

select
    date_trunc('hour', occurred_at) as event_hour,
    producer,
    event_type,
    count(*) as event_count,
    avg(producer_latency_ms) as avg_producer_latency_ms,
    max(ingested_at) as last_ingested_at
from {{ ref('stg_events') }}
group by 1, 2, 3
