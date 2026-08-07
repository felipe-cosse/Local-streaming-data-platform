# Architecture and ownership

## Processing boundary

Flink owns operations that require continuous state: parsing, event time, Kafka offsets, Debezium changelog interpretation, fan-out, checkpointing, and the low-latency sinks. dbt owns bounded relational transformations after records are visible in StarRocks. Airflow schedules bounded jobs and checks but does not supervise a permanent Flink process.

This boundary avoids turning an orchestrator task retry into a second streaming consumer and keeps dbt away from row-by-row stream semantics.

## Storage boundary

StarRocks and Iceberg are not duplicate implementations of one responsibility.

- StarRocks tables are mutable serving projections. Primary-key tables absorb retry duplicates and CDC updates/deletes.
- Iceberg stores durable event history in Parquet, tracks snapshots through the SeaweedFS REST catalog, and remains accessible to other Iceberg engines.
- Kafka provides replay for the configured retention period; it is transport, not the permanent lake.

The first Iceberg path covers application events because they are append-oriented. Adding full CDC history to Iceberg should use explicit audit tables with operation, source timestamp, source offset, before/after data, and a deterministic change ID—not a mutable “current state” table disguised as history.

## Delivery guarantees

| Boundary | Local guarantee | Deduplication / recovery key |
|---|---|---|
| Producers → Kafka | Idempotent producer, at-least-once retries | Kafka producer sequence plus `event_id` |
| Kafka → Flink | Checkpointed Kafka offsets | Flink checkpoint state |
| Flink → Iceberg | Checkpoint-coordinated commits | Iceberg snapshots and Flink state |
| Flink → StarRocks | Retried stream load | StarRocks primary key (`event_id` or source PK) |
| Sources → Debezium | Snapshot plus source log position | Binlog coordinates / PostgreSQL LSN |

No system is described as “exactly once” end to end without naming its boundary. The serving tables are retry-safe because their primary keys converge, while Iceberg commits coordinate with Flink checkpoints.

## Topic policy

Application topics use three partitions to demonstrate key ordering and parallelism. CDC topics also use three partitions locally, but each source table's key determines partition placement. Connect metadata, schema history, and Debezium heartbeat topics are compacted with one replica because the local broker has one node. The heartbeat topics are created explicitly because broker-side automatic topic creation is disabled.

Topic names carry domain and version. Incompatible application contract changes require a new topic suffix. Compatible optional payload additions can stay on v1 after updating the JSON Schema.

## Quality and observability

Soda checks business-facing serving tables hourly. dbt tests model constraints during the same DAG, while the daily reconciliation detects CDC drift that per-table checks cannot see. Prometheus is for component health and throughput; data correctness stays in Soda/dbt so it can be reviewed with the data contract.

## AI trust boundary

The vector index contains operational and semantic metadata, not the entire event warehouse. Retrieval answers “how is this platform defined?” Live counts and measures are queried from StarRocks with a restricted SQL path. Responses return local source paths, and live SQL/results are returned separately from generated prose.

## Production evolution

Keep the logical interfaces while replacing local deployment shapes:

1. Multi-broker Kafka with TLS/SASL, quotas, replication, and a schema registry.
2. Flink high availability, remote checkpoints/savepoints, tested restore procedures, and declarative deployment.
3. Distributed StarRocks with separated roles, backups, resource groups, and workload-specific users.
4. Distributed SeaweedFS or another S3-compatible object store, catalog authentication, lifecycle, and disaster recovery.
5. Airflow secrets backend, remote logs, alert routing, and deployment-managed DAG bundles.
6. Weaviate authentication/RBAC, private models, prompt-injection controls, audited SQL tools, and context retention policy.
