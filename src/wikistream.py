"""Shared client for the Wikimedia recentchange stream (server-sent events)."""

import json
import sys
import time

import requests

STREAM_URL = "https://stream.wikimedia.org/v2/stream/recentchange"
DEFAULT_USER_AGENT = (
    "kafka-wikimedia-pipeline/0.1 "
    "(https://github.com/fowlestravis/kafka-wikimedia-pipeline)"
)

BACKOFF_START = 1.0  # seconds before the first retry
BACKOFF_MAX = 60.0  # never wait longer than this between retries
READ_TIMEOUT = 60.0  # seconds of silence before we treat the connection as dead


def iter_sse(response):
    """Parse an SSE byte stream into (event_id, event_type, data) tuples.

    SSE is line-based: each event is a block of "field: value" lines ended by a
    blank line. Lines starting with ":" are comments (used as keep-alives).
    """
    event_id, event_type, data_lines = None, "message", []
    for raw_line in response.iter_lines(decode_unicode=True):
        if raw_line is None:
            continue
        if raw_line == "":
            # Blank line: dispatch the event we've been building, if any.
            if data_lines:
                yield event_id, event_type, "\n".join(data_lines)
            event_id, event_type, data_lines = None, "message", []
            continue
        if raw_line.startswith(":"):
            continue
        field, _, value = raw_line.partition(":")
        if value.startswith(" "):
            value = value[1:]
        if field == "data":
            data_lines.append(value)
        elif field == "id":
            event_id = value
        elif field == "event":
            event_type = value
        # "retry" and unknown fields are ignored; we manage our own backoff.


def stream_events(user_agent):
    """Yield parsed recentchange events forever, reconnecting on failure.

    On reconnect we send Last-Event-ID so the server resumes where we left off
    instead of skipping or replaying events.
    """
    headers = {"User-Agent": user_agent, "Accept": "text/event-stream"}
    last_event_id = None
    backoff = BACKOFF_START

    while True:
        if last_event_id:
            headers["Last-Event-ID"] = last_event_id
        try:
            with requests.get(
                STREAM_URL,
                headers=headers,
                stream=True,
                timeout=(10, READ_TIMEOUT),
            ) as response:
                response.raise_for_status()
                for event_id, event_type, data in iter_sse(response):
                    if event_type != "message":
                        continue
                    if event_id:
                        last_event_id = event_id
                    try:
                        event = json.loads(data)
                    except json.JSONDecodeError:
                        continue
                    # Wikimedia injects synthetic "canary" events for monitoring.
                    if event.get("meta", {}).get("domain") == "canary":
                        continue
                    backoff = BACKOFF_START  # healthy again, reset the delay
                    yield event
            reason = "server closed the connection"
        except requests.RequestException as exc:
            reason = f"{type(exc).__name__}: {exc}"

        print(f"[reconnect] {reason}; retrying in {backoff:.0f}s", file=sys.stderr)
        time.sleep(backoff)
        backoff = min(backoff * 2, BACKOFF_MAX)
