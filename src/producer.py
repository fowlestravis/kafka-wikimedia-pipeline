"""Publish Wikimedia recentchange events to a Kafka topic.

Usage:
    python src/producer.py              # run until Ctrl-C
    python src/producer.py --count 500  # stop after 500 events
"""

import argparse
import json
import os
import sys
import time

from confluent_kafka import Producer
from dotenv import load_dotenv

from wikistream import DEFAULT_USER_AGENT, stream_events

DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092"
DEFAULT_TOPIC = "recentchange"
STATS_EVERY = 100  # print a stats line after this many produced messages
FLUSH_TIMEOUT = 30.0  # seconds to wait for in-flight messages on shutdown


def build_key(event):
    """Kafka key: the wiki name (e.g. b"enwiki"), or None if it's missing.

    Messages with the same key always land on the same partition, so all
    changes for one wiki stay in order relative to each other.
    """
    wiki = event.get("wiki")
    return wiki.encode("utf-8") if wiki else None


def build_value(event):
    """Kafka value: the event as compact UTF-8 JSON."""
    return json.dumps(event, ensure_ascii=False, separators=(",", ":")).encode("utf-8")


class DeliveryStats:
    """Counts delivery results. `on_delivery` is the producer's callback."""

    def __init__(self, clock=time.monotonic):
        self.clock = clock
        self.started = clock()
        self.produced = 0
        self.sent = 0
        self.failed = 0

    def on_delivery(self, err, msg):
        # Called from producer.poll()/flush(), once per message, after the
        # broker acknowledges it (err is None) or it finally fails.
        if err is None:
            self.sent += 1
        else:
            self.failed += 1
            print(
                f"[delivery failed] key={msg.key()!r} topic={msg.topic()}: {err}",
                file=sys.stderr,
            )

    def summary(self):
        elapsed = max(self.clock() - self.started, 1e-9)
        return (
            f"[stats] produced={self.produced} sent={self.sent} "
            f"failed={self.failed} rate={self.sent / elapsed:.1f} msg/s"
        )


def make_producer(bootstrap_servers):
    return Producer(
        {
            "bootstrap.servers": bootstrap_servers,
            "client.id": "wikimedia-producer",
            "enable.idempotence": True,
            "acks": "all",
            # The compose broker only listens on 127.0.0.1, but "localhost" can
            # resolve to ::1 first, which logs connection-refused noise.
            "broker.address.family": "v4",
        }
    )


def produce(producer, topic, event, stats):
    """Queue one event, waiting for queue space if the local buffer is full."""
    while True:
        try:
            producer.produce(
                topic,
                key=build_key(event),
                value=build_value(event),
                on_delivery=stats.on_delivery,
            )
            break
        except BufferError:
            # Local queue is full: serve callbacks until space frees up.
            producer.poll(1)
    stats.produced += 1


def main():
    load_dotenv()
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--count", type=int, help="stop after N events (default: run until Ctrl-C)"
    )
    args = parser.parse_args()
    if args.count is not None and args.count < 1:
        parser.error("--count must be at least 1")

    bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS") or DEFAULT_BOOTSTRAP_SERVERS
    topic = os.environ.get("KAFKA_TOPIC") or DEFAULT_TOPIC
    user_agent = os.environ.get("WIKI_USER_AGENT") or DEFAULT_USER_AGENT

    producer = make_producer(bootstrap)
    stats = DeliveryStats()
    print(f"Producing to topic {topic!r} on {bootstrap}", file=sys.stderr)

    try:
        for event in stream_events(user_agent):
            produce(producer, topic, event, stats)
            producer.poll(0)  # run delivery callbacks for finished messages
            if stats.produced % STATS_EVERY == 0:
                print(stats.summary(), flush=True)
            if args.count is not None and stats.produced >= args.count:
                break
    except KeyboardInterrupt:
        print("\nInterrupted, flushing...", file=sys.stderr)
    finally:
        remaining = producer.flush(FLUSH_TIMEOUT)
        print(stats.summary(), flush=True)
        if remaining:
            print(f"[warn] {remaining} message(s) not delivered", file=sys.stderr)
            sys.exit(1)


if __name__ == "__main__":
    main()
