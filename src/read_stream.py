"""Read the Wikimedia recentchange stream (server-sent events) and print events.

Usage:
    python src/read_stream.py              # print 20 events
    python src/read_stream.py --count 5    # print 5 events
    python src/read_stream.py --raw        # also dump the first event as JSON
"""

import argparse
import json
import os
from datetime import datetime, timezone

from dotenv import load_dotenv

from wikistream import DEFAULT_USER_AGENT, stream_events


def format_event(event):
    ts = event.get("timestamp")
    when = (
        datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        if isinstance(ts, (int, float))
        else "?"
    )
    bot = "bot" if event.get("bot") else "human"
    return (
        f"{when}  {event.get('wiki', '?'):<14} {event.get('type', '?'):<10} "
        f"{bot:<5}  {event.get('user', '?')} -> {event.get('title', '?')}"
    )


def main():
    load_dotenv()
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--count", type=int, default=20, help="number of events to print (default 20)"
    )
    parser.add_argument(
        "--raw", action="store_true", help="print the full JSON of the first event"
    )
    args = parser.parse_args()
    if args.count < 1:
        parser.error("--count must be at least 1")

    user_agent = os.environ.get("WIKI_USER_AGENT") or DEFAULT_USER_AGENT

    try:
        for i, event in enumerate(stream_events(user_agent)):
            if i == 0 and args.raw:
                print(json.dumps(event, indent=2, ensure_ascii=False))
            print(format_event(event), flush=True)
            if i + 1 >= args.count:
                break
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
