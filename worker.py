#!/usr/bin/env python3
"""Consume and process order events with manual acknowledgements."""

from __future__ import annotations

import argparse
import os
import socket
import time

from messaging import ProcessedEvents, QUEUE_NAME, connection, declare_queue, decode_order


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--name", default=socket.gethostname(), help="worker label")
    parser.add_argument("--delay", type=float, default=1.0, help="processing time in seconds")
    parser.add_argument("--state-file", default="data/processed_events.txt")
    parser.add_argument("--crash-on-event", help="exit before ACK for this event ID")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    events = ProcessedEvents(args.state_file)
    broker = connection()
    channel = broker.channel()
    declare_queue(channel)
    channel.basic_qos(prefetch_count=1)

    def handle_message(channel, method, _properties, body: bytes) -> None:
        try:
            order = decode_order(body)
        except (ValueError, TypeError) as error:
            print(f"[{args.name}] rejected invalid message: {error}", flush=True)
            channel.basic_nack(method.delivery_tag, requeue=False)
            return

        event_id = order["event_id"]
        redelivered = "yes" if method.redelivered else "no"
        print(
            f"[{args.name}] received event={event_id} order={order['order_id']} "
            f"item={order['item_id']} qty={order['quantity']} redelivered={redelivered}",
            flush=True,
        )

        if event_id == args.crash_on_event:
            print(f"[{args.name}] stopping before ACK for event={event_id}", flush=True)
            broker.close()
            os._exit(2)

        if not events.claim(event_id):
            print(f"[{args.name}] duplicate event={event_id}; business action skipped", flush=True)
        else:
            time.sleep(args.delay)
            print(f"[{args.name}] processed order={order['order_id']}", flush=True)

        channel.basic_ack(method.delivery_tag)
        print(f"[{args.name}] ACK event={event_id}", flush=True)

    channel.basic_consume(queue=QUEUE_NAME, on_message_callback=handle_message)
    print(f"[{args.name}] waiting for orders (Ctrl+C to stop)", flush=True)
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        channel.stop_consuming()
    finally:
        if broker.is_open:
            broker.close()


if __name__ == "__main__":
    main()

