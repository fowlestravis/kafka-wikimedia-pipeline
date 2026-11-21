# kafka-wikimedia-pipeline
Kafka streaming pipeline: Wikimedia recent changes -> Postgres -> dbt -> Metabase

## Run Kafka locally

Requires Docker with Compose v2. The broker is a single-node KRaft cluster (no ZooKeeper) listening on `127.0.0.1:9092`.

Start the broker and wait until it is healthy:

```sh
docker compose up -d --wait kafka
```

Create a topic `scratch` with 3 partitions, then describe it:

```sh
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create --topic scratch --partitions 3 --replication-factor 1

docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --describe --topic scratch
```

Produce a few messages from the console:

```sh
printf 'hello\nfrom\nkafka\n' | docker compose exec -T kafka /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 --topic scratch
```

Consume them from the beginning (exits after 3 messages):

```sh
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic scratch \
  --from-beginning --max-messages 3
```

With 3 partitions, messages may come back in a different order than they were produced. Kafka only guarantees order within a single partition.

Tear it down. `-v` also deletes the `kafka_data` volume, which wipes all topics and messages. Leave it off to keep your data:

```sh
docker compose down -v
```

## Read the Wikimedia stream

`src/read_stream.py` connects to Wikimedia's [EventStreams](https://wikitech.wikimedia.org/wiki/Event_Platform/EventStreams) `recentchange` feed and prints one line per edit. It doesn't use Kafka yet.

Requires Python 3.10+. Set up a virtualenv and install dependencies:

```sh
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Optionally set your own User-Agent. Wikimedia asks clients to identify themselves:

```sh
cp .env.example .env   # then edit WIKI_USER_AGENT
```

Print 20 events (the default), or choose how many with `--count`:

```sh
python src/read_stream.py
python src/read_stream.py --count 5
```

Each line shows timestamp (UTC), wiki, change type, bot or human, user, and page title:

```
2026-11-17T21:04:12Z  enwiki         edit       human  ExampleUser -> Some article
```

Add `--raw` to also print the first event as full JSON, so you can see every field:

```sh
python src/read_stream.py --count 1 --raw
```

If the connection drops, the script retries with exponential backoff (1s, 2s, 4s, ... up to 60s) and resumes from the last event it saw. Press Ctrl-C to stop early.

## Produce the stream into Kafka

`src/producer.py` reads the same stream and publishes each event to the Kafka topic `recentchange`. The key is the event's `wiki` (e.g. `enwiki`) and the value is the event's JSON.

Install dependencies as above, and start the broker (see [Run Kafka locally](#run-kafka-locally)).

Create the topic explicitly with 3 partitions. The broker would auto-create it on first write, but with its default settings rather than ours:

```sh
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create --if-not-exists --topic recentchange --partitions 3 --replication-factor 1
```

Run the producer. It runs until Ctrl-C, or stops after `--count` events:

```sh
python src/producer.py
python src/producer.py --count 500
```

Every 100 messages it prints a stats line. `sent` counts messages the broker has acknowledged, so it can lag `produced` a little:

```
[stats] produced=200 sent=199 failed=0 rate=29.7 msg/s
```

On Ctrl-C it waits up to 30 seconds for in-flight messages to be delivered before exiting.

Settings come from the environment or `.env` (see `.env.example`):

| Variable | Default |
| --- | --- |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` |
| `KAFKA_TOPIC` | `recentchange` |
| `WIKI_USER_AGENT` | `kafka-wikimedia-pipeline/0.1 (...)` |

Check that messages arrived, with their key and partition:

```sh
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic recentchange \
  --from-beginning --max-messages 5 \
  --formatter-property print.key=true --formatter-property print.partition=true
```

## Run the tests

The tests don't need a broker:

```sh
pip install -r requirements-dev.txt
pytest
```
