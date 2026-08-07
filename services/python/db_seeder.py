from __future__ import annotations

import logging
import os
import random
import signal
import time
import uuid

import mysql.connector
import psycopg
from faker import Faker


logging.basicConfig(level="INFO", format="%(asctime)s %(levelname)s %(message)s")
LOG = logging.getLogger("db-seeder")
SEED = int(os.getenv("RANDOM_SEED", "42"))
RATE = max(float(os.getenv("DB_MUTATIONS_PER_SECOND", "1")), 0.1)
random.seed(SEED)
faker = Faker()
Faker.seed(SEED)
running = True


def stop(_signum, _frame) -> None:
    global running
    running = False


def connect_with_retry(factory, label: str):
    for attempt in range(1, 61):
        try:
            connection = factory()
            LOG.info("Connected to %s", label)
            return connection
        except Exception as exc:  # connection libraries use different exception trees
            if attempt == 60:
                raise
            LOG.warning("Waiting for %s (%s/60): %s", label, attempt, exc)
            time.sleep(2)
    raise RuntimeError(f"Could not connect to {label}")


def mysql_connection():
    return mysql.connector.connect(
        host=os.getenv("MYSQL_HOST", "mysql"),
        port=3306,
        database=os.getenv("MYSQL_DATABASE", "inventory"),
        user=os.getenv("MYSQL_USER", "app_user"),
        password=os.getenv("MYSQL_PASSWORD", "local_app_password"),
        autocommit=True,
    )


def postgres_connection():
    return psycopg.connect(
        host=os.getenv("POSTGRES_HOST", "postgres-source"),
        port=5432,
        dbname=os.getenv("POSTGRES_DATABASE", "appdb"),
        user=os.getenv("POSTGRES_USER", "app_user"),
        password=os.getenv("POSTGRES_PASSWORD", "local_app_password"),
        autocommit=True,
    )


def mutate_mysql(connection) -> None:
    operation = random.choices(["insert", "update", "delete"], [0.65, 0.3, 0.05])[0]
    cursor = connection.cursor()
    try:
        if operation == "insert":
            cursor.execute(
                "INSERT INTO orders (customer_id, status, amount) VALUES (%s, %s, %s)",
                (
                    random.randint(1, 250),
                    random.choice(["created", "paid", "shipped"]),
                    round(random.uniform(5, 1000), 2),
                ),
            )
        elif operation == "update":
            cursor.execute(
                "UPDATE orders SET status = %s WHERE id = (SELECT id FROM "
                "(SELECT id FROM orders ORDER BY RAND() LIMIT 1) AS candidate)",
                (random.choice(["paid", "shipped", "cancelled"]),),
            )
        else:
            cursor.execute("DELETE FROM orders WHERE id > 1 ORDER BY id LIMIT 1")
    finally:
        cursor.close()


def mutate_postgres(connection) -> None:
    operation = random.choices(["insert", "update", "delete"], [0.65, 0.3, 0.05])[0]
    with connection.cursor() as cursor:
        if operation == "insert":
            cursor.execute(
                "INSERT INTO customers (email, full_name, status) VALUES (%s, %s, %s)",
                (
                    f"{uuid.uuid4().hex[:12]}@example.test",
                    faker.name(),
                    random.choice(["active", "trial", "inactive"]),
                ),
            )
        elif operation == "update":
            cursor.execute(
                "UPDATE customers SET status = %s, updated_at = NOW() "
                "WHERE id = (SELECT id FROM customers ORDER BY random() LIMIT 1)",
                (random.choice(["active", "inactive", "suspended"]),),
            )
        else:
            cursor.execute(
                "DELETE FROM customers WHERE id = "
                "(SELECT id FROM customers WHERE id > 1 ORDER BY id LIMIT 1)"
            )


def main() -> None:
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    mysql_db = connect_with_retry(mysql_connection, "MySQL")
    postgres_db = connect_with_retry(postgres_connection, "PostgreSQL")
    interval = 1 / RATE

    while running:
        started = time.monotonic()
        try:
            if not mysql_db.is_connected():
                mysql_db = connect_with_retry(mysql_connection, "MySQL")
            mutate_mysql(mysql_db)
        except Exception as exc:
            LOG.warning("MySQL mutation failed; reconnecting: %s", exc)
            mysql_db = connect_with_retry(mysql_connection, "MySQL")

        try:
            if postgres_db.closed:
                postgres_db = connect_with_retry(postgres_connection, "PostgreSQL")
            mutate_postgres(postgres_db)
        except Exception as exc:
            LOG.warning("PostgreSQL mutation failed; reconnecting: %s", exc)
            postgres_db = connect_with_retry(postgres_connection, "PostgreSQL")

        remaining = interval - (time.monotonic() - started)
        if remaining > 0:
            time.sleep(remaining)

    mysql_db.close()
    postgres_db.close()


if __name__ == "__main__":
    main()
