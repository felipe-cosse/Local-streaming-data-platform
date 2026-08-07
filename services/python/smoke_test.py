from __future__ import annotations

import os
import sys

import pymysql
import requests
from confluent_kafka.admin import AdminClient


failures: list[str] = []


def check(name: str, action, required: bool = True) -> None:
    try:
        detail = action()
        print(f"PASS {name}: {detail if detail is not None else 'ok'}")
    except Exception as exc:
        prefix = "FAIL" if required else "SKIP"
        print(f"{prefix} {name}: {exc}")
        if required:
            failures.append(f"{name}: {exc}")


def kafka_check() -> str:
    admin = AdminClient(
        {"bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")}
    )
    topics = set(admin.list_topics(timeout=10).topics)
    expected = {
        "app.python.events.v1",
        "app.php.events.v1",
        "cdc.mysql.inventory.orders",
        "cdc.postgres.public.customers",
        "__debezium-heartbeat.cdc.mysql",
        "__debezium-heartbeat.cdc.postgres",
    }
    missing = expected - topics
    if missing:
        raise AssertionError(f"missing topics: {sorted(missing)}")
    return f"{len(topics)} topics"


def connectors_check() -> str:
    base = os.getenv("CONNECT_URL", "http://connect:8083")
    connectors = requests.get(f"{base}/connectors", timeout=5).json()
    for connector in ("mysql-orders", "postgres-customers"):
        if connector not in connectors:
            raise AssertionError(f"missing connector {connector}")
        status = requests.get(f"{base}/connectors/{connector}/status", timeout=5).json()
        connector_state = status.get("connector", {}).get("state")
        task_states = [task.get("state") for task in status.get("tasks", [])]
        if connector_state != "RUNNING" or any(state != "RUNNING" for state in task_states):
            raise AssertionError(f"{connector} state={connector_state} tasks={task_states}")
    return ", ".join(connectors)


def starrocks_check() -> str:
    connection = pymysql.connect(
        host=os.getenv("STARROCKS_HOST", "starrocks"),
        port=int(os.getenv("STARROCKS_PORT", "9030")),
        user="root",
        password="",
        connect_timeout=5,
    )
    with connection:
        with connection.cursor() as cursor:
            cursor.execute("SHOW TABLES FROM analytics")
            tables = {row[0] for row in cursor.fetchall()}
            required = {"events_realtime", "orders_current", "customers_current"}
            if not required.issubset(tables):
                raise AssertionError(f"missing tables: {sorted(required - tables)}")
            cursor.execute("SELECT COUNT(*) FROM analytics.events_realtime")
            count = cursor.fetchone()[0]
    return f"tables ready, events={count}"


def http_health(url: str) -> str:
    response = requests.get(url, timeout=5)
    response.raise_for_status()
    return str(response.status_code)


def main() -> None:
    check("Kafka", kafka_check)
    check("Debezium connectors", connectors_check)
    check("StarRocks", starrocks_check)
    check(
        "SeaweedFS Iceberg",
        lambda: http_health(
            os.getenv("ICEBERG_CATALOG_URL", "http://seaweedfs:8181")
            + "/v1/config?warehouse=s3://iceberg-warehouse/"
        ),
        required=False,
    )
    check(
        "Flink",
        lambda: http_health(os.getenv("FLINK_URL", "http://flink-jobmanager:8081") + "/overview"),
        required=False,
    )
    check(
        "Weaviate",
        lambda: http_health(os.getenv("WEAVIATE_URL", "http://weaviate:8080") + "/v1/.well-known/ready"),
        required=False,
    )
    check(
        "Airflow",
        lambda: http_health(os.getenv("AIRFLOW_URL", "http://airflow-api-server:8080") + "/api/v2/monitor/health"),
        required=False,
    )
    check(
        "Prometheus",
        lambda: http_health(os.getenv("PROMETHEUS_URL", "http://prometheus:9090") + "/-/ready"),
        required=False,
    )
    if failures:
        print("\nRequired smoke checks failed:")
        for failure in failures:
            print(f"- {failure}")
        sys.exit(1)
    print("\nRequired smoke checks passed")


if __name__ == "__main__":
    main()
