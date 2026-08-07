# Local runbook

## Docker reports no space during first startup

The pinned images are intentionally complete and the first pull is large, especially StarRocks, Debezium Connect, Flink, and Airflow. Keep at least 30 GB free inside Docker's own storage allocation, not only on the host filesystem. On Docker Desktop, enlarge or clean the virtual disk before retrying. Do not repeatedly restart partially initialized databases: run `make clean` first if the failed attempt created new, disposable volumes, then rerun `make up-core` after space is available.

## Normal startup

1. `make up-core`
2. Confirm required containers with `make status`.
3. `make up-lakehouse`
4. Wait about one minute for checkpoints and stream-load buffers.
5. `make smoke`
6. Start only the optional profile needed for the current task.

Bootstrap is idempotent: it creates missing Kafka topics, runs `CREATE IF NOT EXISTS` DDL, updates both connector configurations, and creates the Iceberg namespace if needed.

## CDC connector is failed

Inspect the complete status and recent logs:

```bash
curl -sS http://localhost:8083/connectors/mysql-orders/status
curl -sS http://localhost:8083/connectors/postgres-customers/status
docker compose logs --tail=200 connect mysql postgres-source
```

Typical local causes are credentials changed after a database volume was initialized, a PostgreSQL replication slot left by a different connector definition, or a MySQL binlog setting removed. For credential changes, either restore the original `.env` value or intentionally run `make clean` and reinitialize all generated local data.

After correcting configuration, rerun `make bootstrap`. The connector `PUT` operation updates rather than duplicates it.

## Flink job is failed

Use the Flink UI at http://localhost:8081, then inspect logs:

```bash
docker compose logs --tail=300 flink-jobmanager flink-taskmanager
```

Common checks:

- Kafka, StarRocks, and SeaweedFS are healthy.
- The Iceberg catalog responds at `http://localhost:8181/v1/config?warehouse=s3://iceberg-warehouse/`.
- The local S3 credentials in `.env` match those used when SeaweedFS initialized.
- StarRocks tables exist (`make bootstrap`).

Cancel a failed job in the UI and run `make submit-flink`. The submission script skips pipeline names already present in the running-jobs response.

## StarRocks has no events

Check one boundary at a time:

1. Producer logs show successful delivery.
2. Kafka topic offsets increase in Grafana or with Kafka CLI.
3. Flink shows the `events-to-serving-and-lakehouse` job as RUNNING.
4. Flink task logs show successful StarRocks stream loads.
5. Query `SELECT producer, COUNT(*) FROM analytics.events_realtime GROUP BY producer;`.

This sequence distinguishes generation, transport, processing, and serving failures without guessing.

## Quality scan fails

Run `make quality` to reproduce outside Airflow. A freshness failure usually means the event job or producers stopped; key or valid-value failures are contract/model issues. A row-count failure during initial startup can be transient—wait for one Flink buffer flush and rerun.

## RAG indexing fails

Run `make ai-models` first. Verify the embedding model appears in `curl -sS http://localhost:11434/api/tags`, then rerun `make rag-index`. The index refresh replaces the collection as one bounded operation; the previous collection is removed before re-embedding local sources.

## Reset policy

`make down` preserves local state. `make clean` removes every named volume, including source rows, Kafka logs, checkpoints, Iceberg objects, StarRocks data, Airflow metadata, vectors, models, and metrics. Treat `make clean` as destructive and use it only when a full local reset is intended.
