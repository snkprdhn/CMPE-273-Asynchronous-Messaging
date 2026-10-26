"""Shared RabbitMQ and order-message helpers."""

from __future__ import annotations

import json
import os
import fcntl
from pathlib import Path

import pika

QUEUE_NAME = "orders"
REQUIRED_FIELDS = {"event_id", "order_id", "item_id", "quantity"}


def connection() -> pika.BlockingConnection:
    url = os.getenv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/%2F")
    return pika.BlockingConnection(pika.URLParameters(url))


def declare_queue(channel: pika.channel.Channel) -> None:
    channel.queue_declare(queue=QUEUE_NAME, durable=True)


def decode_order(body: bytes) -> dict:
    order = json.loads(body)
    if not isinstance(order, dict):
        raise ValueError("message must be a JSON object")
    missing = REQUIRED_FIELDS - order.keys()
    if missing:
        raise ValueError(f"missing fields: {', '.join(sorted(missing))}")
    if not isinstance(order["quantity"], int) or order["quantity"] < 1:
        raise ValueError("quantity must be a positive integer")
    return order


class ProcessedEvents:
    """Small file-backed set shared by local worker processes."""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)

    def claim(self, event_id: str) -> bool:
        """Record a new event atomically; return False if it was already seen."""
        self.path.parent.mkdir(parents=True, exist_ok=True)
        lock_path = self.path.with_suffix(self.path.suffix + ".lock")
        with lock_path.open("w") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            events = self._read()
            if event_id in events:
                return False
            events.add(event_id)
            temporary = self.path.with_suffix(f".{os.getpid()}.tmp")
            temporary.write_text("\n".join(sorted(events)) + "\n")
            temporary.replace(self.path)
            return True

    def _read(self) -> set[str]:
        if not self.path.exists():
            return set()
        return set(self.path.read_text().splitlines())

