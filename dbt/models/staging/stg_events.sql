select
    event_id,
    event_type,
    event_version,
    producer,
    occurred_at,
    ingested_at,
    correlation_id,
    partition_key,
    payload_json,
    source_topic,
    kafka_partition,
    kafka_offset,
    timestampdiff(millisecond, occurred_at, ingested_at) as producer_latency_ms
from {{ source('realtime', 'events_realtime') }}
where event_id is not null
  and occurred_at is not null
