# ADR 0001: Local open-source streaming platform

Status: accepted

## Decision

Use Apache Kafka in single-node KRaft mode for the local transport, Flink for long-running processing, StarRocks for mutable low-latency serving, and Apache Iceberg on the SeaweedFS built-in S3/REST catalog for durable history. Use dbt Core for bounded warehouse transformations, Soda Core 3.x for quality, Airflow with LocalExecutor for scheduling, and Weaviate plus Ollama for local context/RAG.

## Consequences

The architecture is API-compatible with common distributed production shapes while remaining fully local. It is more resource-intensive than a minimal demo, so optional profiles are a first-class part of the design. The single-node services are development references and provide no local high availability.

Soda is pinned to its open-source 3.x connector line. The quality datasource uses the StarRocks MySQL-compatible protocol. SeaweedFS removes the need for a second local object-storage or catalog service.

Airflow, Soda, and the RAG API use separate images because their Python dependency graphs and operational lifecycles are different. Airflow triggers Soda and context refreshes over private Compose-network HTTP endpoints; neither helper API publishes a host port.
