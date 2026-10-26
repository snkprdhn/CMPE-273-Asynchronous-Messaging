#!/usr/bin/env python3
"""Publish sample order events to RabbitMQ."""

from __future__ import annotations

import argparse
import json
import uuid

import pika

from messaging import QUEUE_NAME, connection, declare_queue


def make_order(number: int, event_id: str | None = None) -> dict:
    return {
        "event_id": event_id or str(uuid.uuid4()),
        "order_id": f"order-{number:03d}",
        "item_id": f"item-{(number % 4) + 1:02d}",
        "quantity": (number % 3) + 1,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--count", type=int, default=8, help="number of orders")
    parser.add_argument("--event-id", help="use one event ID (requires --count 1)")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.count < 1:
        raise SystemExit("--count must be at least 1")
    if args.event_id and args.count != 1:
        raise SystemExit("--event-id requires --count 1")

    broker = connection()
    channel = broker.channel()
    declare_queue(channel)

    for number in range(1, args.count + 1):
        order = make_order(number, args.event_id)
        channel.basic_publish(
            exchange="",
            routing_key=QUEUE_NAME,
            body=json.dumps(order),
            properties=pika.BasicProperties(
                content_type="application/json",
                delivery_mode=pika.DeliveryMode.Persistent,
            ),
        )
        print(
            f"[producer] sent event={order['event_id']} "
            f"order={order['order_id']} item={order['item_id']} qty={order['quantity']}"
        )

    broker.close()


if __name__ == "__main__":
    main()

