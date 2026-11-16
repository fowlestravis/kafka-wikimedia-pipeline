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
