from __future__ import annotations

import json
import logging
import os
import random
import signal
import time
import uuid
from datetime import UTC, datetime
from pathlib import Path

from confluent_kafka import KafkaException, Producer
from faker import Faker
from jsonschema import Draft202012Validator, FormatChecker


logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(message)s",
)
LOG = logging.getLogger("event-producer")

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
TOPIC = os.getenv("KAFKA_TOPIC", "app.python.events.v1")
PRODUCER_NAME = os.getenv("PRODUCER_NAME", "python-generator")
RATE = max(float(os.getenv("EVENTS_PER_SECOND", "2")), 0.1)
SEED = int(os.getenv("RANDOM_SEED", "42"))

random.seed(SEED)
faker = Faker()
Faker.seed(SEED)
validator = Draft202012Validator(
    json.loads(Path("/contracts/event-v1.schema.json").read_text()),
    format_checker=FormatChecker(),
)
running = True


def utc_now() -> str:
    return datetime.now(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def build_event() -> dict[str, object]:
    event_type = random.choice(
        ["user.activity", "order.checkout", "system.log", "application.metric"]
    )
    partition_key = str(random.randint(1, 100))
    payloads = {
        "user.activity": {
            "user_id": partition_key,
            "action": random.choice(["login", "search", "view", "logout"]),
            "ip": faker.ipv4_public(),
        },
        "order.checkout": {
            "customer_id": partition_key,
            "amount": f"{random.uniform(5, 500):.2f}",
            "currency": "USD",
        },
        "system.log": {
            "level": random.choice(["INFO", "WARN", "ERROR"]),
            "component": random.choice(["api", "worker", "billing"]),
            "message": faker.sentence(nb_words=7),
        },
        "application.metric": {
            "metric": random.choice(["latency_ms", "queue_depth", "request_count"]),
            "value": f"{random.uniform(1, 1000):.3f}",
            "unit": "count",
        },
    }
    timestamp = utc_now()
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "event_version": 1,
        "producer": PRODUCER_NAME,
        "occurred_at": timestamp,
        "ingested_at": timestamp,
        "correlation_id": str(uuid.uuid4()),
        "partition_key": partition_key,
        "payload": payloads[event_type],
    }


def delivery_report(error, message) -> None:
    if error is not None:
        LOG.error("Kafka delivery failed: %s", error)
    else:
        LOG.debug(
            "Delivered %s[%s] at offset %s",
            message.topic(),
            message.partition(),
            message.offset(),
        )


def stop(_signum, _frame) -> None:
    global running
    running = False


def main() -> None:
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    producer = Producer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "client.id": PRODUCER_NAME,
            "enable.idempotence": True,
            "acks": "all",
            "compression.type": "zstd",
            "retries": 10,
        }
    )
    interval = 1 / RATE
    LOG.info("Producing %.2f events/s to %s", RATE, TOPIC)

    while running:
        started = time.monotonic()
        event = build_event()
        validator.validate(event)
        try:
            producer.produce(
                TOPIC,
                key=str(event["partition_key"]).encode(),
                value=json.dumps(event, separators=(",", ":")).encode(),
                on_delivery=delivery_report,
            )
            producer.poll(0)
        except BufferError:
            producer.poll(1)
        except KafkaException as exc:
            LOG.warning("Kafka unavailable, retrying: %s", exc)
            time.sleep(2)

        remaining = interval - (time.monotonic() - started)
        if remaining > 0:
            time.sleep(remaining)

    LOG.info("Stopping producer and flushing pending records")
    undelivered = producer.flush(10)
    if undelivered:
        LOG.warning("%s records were not delivered before shutdown", undelivered)


if __name__ == "__main__":
    main()
