from __future__ import annotations

import json
import os
import re
import time
from pathlib import Path

import pymysql
import requests
from confluent_kafka.admin import (
    AdminClient,
    AlterConfigOpType,
    ConfigEntry,
    ConfigResource,
    NewTopic,
)


TOPICS = {
    "app.python.events.v1": (3, {}),
    "app.php.events.v1": (3, {}),
    "cdc.mysql.inventory.orders": (3, {}),
    "cdc.postgres.public.customers": (3, {}),
    "__debezium-heartbeat.cdc.mysql": (1, {"cleanup.policy": "compact"}),
    "__debezium-heartbeat.cdc.postgres": (1, {"cleanup.policy": "compact"}),
    "curated.events.v1": (3, {}),
    "curated.orders.v1": (3, {}),
    "quarantine.invalid-events.v1": (1, {"retention.ms": "604800000"}),
    "schema-history.mysql.inventory": (
        1,
        {"cleanup.policy": "delete", "retention.ms": "-1"},
    ),
    "connect-configs": (1, {"cleanup.policy": "compact"}),
    "connect-offsets": (1, {"cleanup.policy": "compact"}),
    "connect-status": (1, {"cleanup.policy": "compact"}),
}


def wait_for(label: str, action, attempts: int = 60, delay: float = 2):
    last_error = None
    for attempt in range(1, attempts + 1):
        try:
            return action()
        except Exception as exc:
            last_error = exc
            print(f"Waiting for {label} ({attempt}/{attempts}): {exc}")
            time.sleep(delay)
    raise RuntimeError(f"{label} did not become ready: {last_error}")


def create_topics() -> None:
    admin = AdminClient(
        {"bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")}
    )
    wait_for("Kafka", lambda: admin.list_topics(timeout=5))
    existing = set(admin.list_topics(timeout=10).topics)
    missing = [
        NewTopic(name, num_partitions=partitions, replication_factor=1, config=config)
        for name, (partitions, config) in TOPICS.items()
        if name not in existing
    ]
    if missing:
        for name, future in admin.create_topics(missing).items():
            future.result(timeout=30)
            print(f"Created Kafka topic {name}")
    else:
        print("Kafka topics already exist")

    resources = []
    for name, (_, config) in TOPICS.items():
        if not config:
            continue
        entries = [
            ConfigEntry(key, value, incremental_operation=AlterConfigOpType.SET)
            for key, value in config.items()
        ]
        resources.append(
            ConfigResource(
                ConfigResource.Type.TOPIC,
                name,
                incremental_configs=entries,
            )
        )
    for resource, future in admin.incremental_alter_configs(resources).items():
        future.result(timeout=30)
        print(f"Reconciled Kafka topic configuration {resource.name}")


def split_sql(source: str) -> list[str]:
    without_comments = re.sub(r"(?m)^\s*--.*$", "", source)
    return [statement.strip() for statement in without_comments.split(";") if statement.strip()]


def initialize_starrocks() -> None:
    reader_user = os.getenv("STARROCKS_READER_USER", "analytics_reader")
    reader_password = os.getenv("STARROCKS_READER_PASSWORD", "local_analytics_reader")
    sql = Path("/sql/starrocks/01-platform.sql").read_text()
    sql = sql.replace("{{STARROCKS_READER_USER}}", reader_user)
    sql = sql.replace("{{STARROCKS_READER_PASSWORD}}", reader_password)

    def connect():
        return pymysql.connect(
            host=os.getenv("STARROCKS_HOST", "starrocks"),
            port=int(os.getenv("STARROCKS_PORT", "9030")),
            user=os.getenv("STARROCKS_USER", "root"),
            password=os.getenv("STARROCKS_PASSWORD", ""),
            autocommit=True,
            connect_timeout=5,
        )

    connection = wait_for("StarRocks", connect, attempts=90)
    with connection:
        with connection.cursor() as cursor:
            for statement in split_sql(sql):
                cursor.execute(statement)
    print("Initialized StarRocks databases, tables, and read-only user")


def register_connectors() -> None:
    connect_url = os.getenv("CONNECT_URL", "http://connect:8083")
    wait_for(
        "Kafka Connect",
        lambda: requests.get(f"{connect_url}/connectors", timeout=5).raise_for_status(),
    )
    replacements = {
        "mysql-orders": {
            "database.user": os.getenv("MYSQL_DEBEZIUM_USER", "debezium"),
            "database.password": os.getenv(
                "MYSQL_DEBEZIUM_PASSWORD", "local_debezium_password"
            ),
            "database.include.list": os.getenv("MYSQL_DATABASE", "inventory"),
            "table.include.list": f"{os.getenv('MYSQL_DATABASE', 'inventory')}.orders",
        },
        "postgres-customers": {
            "database.user": os.getenv("POSTGRES_DEBEZIUM_USER", "debezium"),
            "database.password": os.getenv(
                "POSTGRES_DEBEZIUM_PASSWORD", "local_debezium_password"
            ),
            "database.dbname": os.getenv("POSTGRES_DB", "appdb"),
        },
    }
    for name in ("mysql-orders", "postgres-customers"):
        config = json.loads(Path(f"/config/debezium/{name}.json").read_text())
        config.update(replacements[name])
        response = requests.put(
            f"{connect_url}/connectors/{name}/config", json=config, timeout=30
        )
        response.raise_for_status()
        print(f"Registered or updated Debezium connector {name}")

        restart = requests.post(
            f"{connect_url}/connectors/{name}/restart",
            params={"includeTasks": "true", "onlyFailed": "true"},
            timeout=30,
        )
        if restart.status_code not in (200, 202, 204):
            restart.raise_for_status()

    def connector_ready(name: str) -> str:
        response = requests.get(f"{connect_url}/connectors/{name}/status", timeout=5)
        response.raise_for_status()
        status = response.json()
        connector_state = status.get("connector", {}).get("state")
        task_states = [task.get("state") for task in status.get("tasks", [])]
        if connector_state != "RUNNING" or not task_states or any(
            state != "RUNNING" for state in task_states
        ):
            raise RuntimeError(
                f"connector={connector_state}, tasks={task_states or ['not-created']}"
            )
        return name

    for name in ("mysql-orders", "postgres-customers"):
        wait_for(
            f"Debezium connector {name}",
            lambda connector_name=name: connector_ready(connector_name),
            attempts=45,
        )
        print(f"Debezium connector {name} is running")


def initialize_iceberg() -> None:
    if os.getenv("BOOTSTRAP_LAKEHOUSE", "true").lower() != "true":
        print("Skipping optional Iceberg bootstrap")
        return
    base_url = os.getenv("ICEBERG_CATALOG_URL", "http://seaweedfs:8181")
    warehouse = os.getenv("ICEBERG_WAREHOUSE", "s3://iceberg-warehouse/")
    access_key = os.getenv("AWS_ACCESS_KEY_ID", "seaweedfs")
    secret_key = os.getenv("AWS_SECRET_ACCESS_KEY", "local_seaweedfs_secret")

    wait_for(
        "SeaweedFS Iceberg catalog",
        lambda: requests.get(
            f"{base_url}/v1/config", params={"warehouse": warehouse}, timeout=5
        ).raise_for_status(),
    )
    token_response = requests.post(
        f"{base_url}/v1/oauth/tokens",
        data={
            "grant_type": "client_credentials",
            "client_id": access_key,
            "client_secret": secret_key,
            "scope": "catalog",
        },
        timeout=10,
    )
    token_response.raise_for_status()
    token = token_response.json()["access_token"]
    response = requests.post(
        f"{base_url}/v1/iceberg-warehouse/namespaces",
        headers={"Authorization": f"Bearer {token}"},
        json={"namespace": ["bronze"], "properties": {}},
        timeout=10,
    )
    if response.status_code not in (200, 201, 409):
        response.raise_for_status()
    print("SeaweedFS Iceberg namespace bronze is ready")


def main() -> None:
    create_topics()
    initialize_starrocks()
    register_connectors()
    initialize_iceberg()
    print("Platform bootstrap completed")


if __name__ == "__main__":
    main()
