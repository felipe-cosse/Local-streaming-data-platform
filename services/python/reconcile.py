from __future__ import annotations

import os
import sys
import time

import mysql.connector
import psycopg
import pymysql


MAX_DELTA = int(os.getenv("RECONCILIATION_MAX_DELTA", "10"))


def source_counts() -> tuple[int, int]:
    mysql_db = mysql.connector.connect(
        host=os.getenv("MYSQL_HOST", "mysql"),
        database=os.getenv("MYSQL_DATABASE", "inventory"),
        user=os.getenv("MYSQL_USER", "app_user"),
        password=os.getenv("MYSQL_PASSWORD", "local_app_password"),
    )
    with mysql_db.cursor() as cursor:
        cursor.execute("SELECT COUNT(*) FROM orders")
        orders = cursor.fetchone()[0]
    mysql_db.close()

    postgres_db = psycopg.connect(
        host=os.getenv("POSTGRES_HOST", "postgres-source"),
        dbname=os.getenv("POSTGRES_DATABASE", "appdb"),
        user=os.getenv("POSTGRES_USER", "app_user"),
        password=os.getenv("POSTGRES_PASSWORD", "local_app_password"),
    )
    with postgres_db.cursor() as cursor:
        cursor.execute("SELECT COUNT(*) FROM customers")
        customers = cursor.fetchone()[0]
    postgres_db.close()
    return orders, customers


def target_counts() -> tuple[int, int]:
    starrocks = pymysql.connect(
        host=os.getenv("STARROCKS_HOST", "starrocks"),
        port=int(os.getenv("STARROCKS_PORT", "9030")),
        user=os.getenv("STARROCKS_READER_USER", "analytics_reader"),
        password=os.getenv("STARROCKS_READER_PASSWORD", "local_analytics_reader"),
    )
    with starrocks:
        with starrocks.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM analytics.orders_current")
            orders = cursor.fetchone()[0]
            cursor.execute("SELECT COUNT(*) FROM analytics.customers_current")
            customers = cursor.fetchone()[0]
    return orders, customers


def main() -> None:
    for attempt in range(1, 6):
        source_orders, source_customers = source_counts()
        target_orders, target_customers = target_counts()
        order_delta = abs(source_orders - target_orders)
        customer_delta = abs(source_customers - target_customers)
        print(
            "reconciliation "
            f"orders={source_orders}/{target_orders} delta={order_delta}; "
            f"customers={source_customers}/{target_customers} delta={customer_delta}"
        )
        if order_delta <= MAX_DELTA and customer_delta <= MAX_DELTA:
            return
        if attempt < 5:
            time.sleep(15)
    print(f"Reconciliation exceeded the allowed delta of {MAX_DELTA}", file=sys.stderr)
    sys.exit(1)


if __name__ == "__main__":
    main()
