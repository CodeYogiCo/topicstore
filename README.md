# topicstore

A small platform that **ingests data from many Kafka topics into ClickHouse** and gives you a web UI to **add/remove topics on the fly** and **look up events**.

- **Backend**: Micronaut + Kotlin. Single tenant of one ClickHouse, one Kafka cluster. Dynamic subscription manager — add a topic via the UI, the backend re-subscribes within seconds.
- **UI**: Kotlin/JS + React (kotlin-react wrappers). Two pages: Topics (CRUD) and Lookup (filter by topic / key / time range / JSON path). Served as a static bundle by nginx, with `/api/*` proxied to the backend.
- **Store**: ClickHouse — one wide `kafka_events` table with raw JSON payload (`JSONExtract*` for ad-hoc querying), plus a `topic_registry` table that is the source of truth for which topics to subscribe to.
- **Infra**: Everything ships as Docker images. `docker compose up` brings up Zookeeper + Kafka + ClickHouse + backend + UI. CI uses the same compose stack for end-to-end functional tests.

---

## Architecture

```
                            ┌─────────────────────────┐
                            │  UI  (Kotlin/JS+React)  │   :8081 -> nginx
                            └────────────┬────────────┘
                                         │ /api/* (proxied)
                                         ▼
 ┌─────────┐    produce    ┌──────────────────────────┐    INSERT     ┌──────────────┐
 │ Apps /  │ ────────────► │   Kafka (Zookeeper)      │               │  ClickHouse  │
 │ tests   │               │   bitnami/kafka:3.7      │               │  24.3        │
 └─────────┘               └────────────┬─────────────┘               └──────▲───────┘
                                        │ subscribe                          │
                                        ▼                                    │
                            ┌──────────────────────────┐                     │
                            │ Backend (Micronaut/Ktlin)│ ───────── batch ────┘
                            │  - ConsumerManager       │
                            │  - TopicRegistry         │  (registry stored
                            │  - REST /api             │   in ClickHouse)
                            └──────────────────────────┘
```

### Data model (ClickHouse)

```sql
CREATE TABLE kafka_events (
    topic     String,
    partition Int32,
    offset    Int64,
    `key`     Nullable(String),
    ts        DateTime64(3) DEFAULT now64(3),   -- ingest time
    kafka_ts  DateTime64(3),                    -- record timestamp from Kafka
    payload   String                            -- raw value (JSON or otherwise)
) ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(ts)
ORDER BY (topic, ts, partition, offset);

CREATE TABLE topic_registry (
    name       String,
    created_at DateTime64(3),
    active     UInt8,
    version    DateTime64(3)
) ENGINE = ReplacingMergeTree(version)
ORDER BY name;
```

`payload` is a raw `String`. Use ClickHouse JSON functions to query it: `JSONExtractString(payload, 'user.id')`, `JSONExtractInt(payload, 'amount')`, etc.

---

## Quickstart

Requires Docker (and Docker Compose v2, bundled with modern Docker Desktop).

```bash
docker compose up --build -d
```

Wait ~30–60s for the backend health check to go green:

```bash
curl -s http://localhost:8080/api/health | jq
```

Open the UI: **http://localhost:8081**

Service ports:

| Service     | Host port | Notes                       |
|-------------|-----------|-----------------------------|
| UI (nginx)  | 8081      | proxies `/api/*` to backend |
| Backend     | 8080      | Micronaut HTTP              |
| Kafka       | 9092      | internal listener           |
|             | 9094      | external listener (host)    |
| ClickHouse  | 8123      | HTTP interface              |
|             | 9000      | native TCP                  |
| Zookeeper   | 2181      |                             |

---

## Adding topics

### Via UI

Open **http://localhost:8081**, go to **Topics**, type a name, click **Add**. The backend re-subscribes within ~5s (configurable via `INGEST_REFRESH_MS`).

### Via REST

```bash
curl -X POST -H 'content-type: application/json' \
    -d '{"name":"events.orders"}' \
    http://localhost:8080/api/topics

curl -s http://localhost:8080/api/topics | jq

curl -X DELETE http://localhost:8080/api/topics/events.orders
```

Topics persist in ClickHouse (`topic_registry`) so the subscription list survives restarts.

---

## Producing test data

```bash
docker exec -i topicstore-kafka bash -c \
  'kafka-console-producer.sh --bootstrap-server localhost:9092 --topic events.orders \
     --property parse.key=true --property key.separator=:' <<'EOF'
o-1:{"order_id":"o-1","user":{"id":"u1"},"amount":42}
o-2:{"order_id":"o-2","user":{"id":"u2"},"amount":17}
o-3:{"order_id":"o-3","user":{"id":"u1"},"amount":99}
EOF
```

---

## Querying

### Via UI

Go to **Lookup**. Pick a topic, optional key, time range, or JSON path / value (e.g. path `user.id`, value `u1`).

### Via REST

```bash
# all rows for a topic
curl -s 'http://localhost:8080/api/lookup?topic=events.orders&limit=10' | jq

# by key
curl -s 'http://localhost:8080/api/lookup?topic=events.orders&key=o-2' | jq

# by JSON field (uses ClickHouse JSONExtractString)
curl -s 'http://localhost:8080/api/lookup?topic=events.orders&jsonPath=user.id&jsonValue=u1' | jq

# by time range (ISO-8601 UTC)
curl -s 'http://localhost:8080/api/lookup?topic=events.orders&from=2025-01-01T00:00:00Z&to=2026-01-01T00:00:00Z' | jq
```

### Direct ClickHouse

```bash
docker exec -it topicstore-clickhouse clickhouse-client -q \
  "SELECT topic, count() FROM kafka_events GROUP BY topic ORDER BY topic FORMAT PrettyCompactMonoBlock"
```

---

## Configuration

All backend config is driven by environment variables (read by `application.yml`):

| Env var                  | Default                                  | Meaning                                |
|--------------------------|------------------------------------------|----------------------------------------|
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka:9092`                             | Kafka brokers                          |
| `KAFKA_GROUP_ID`         | `topicstore-consumer`                    | Consumer group                         |
| `CLICKHOUSE_URL`         | `jdbc:ch://clickhouse:8123/default`      | JDBC URL                               |
| `CLICKHOUSE_USER`        | `default`                                | ClickHouse user                        |
| `CLICKHOUSE_PASSWORD`    | _empty_                                  | ClickHouse password                    |
| `INGEST_REFRESH_MS`      | `5000`                                   | How often consumer reconciles topic list |

---

## CI (GitHub Actions)

`.github/workflows/ci.yml` runs three jobs on every push/PR:

1. **unit-tests** — runs Testcontainers-based JUnit5 tests; spins up disposable Kafka + ClickHouse containers in-process and exercises the full ingest + REST + lookup path.
2. **build-images** — builds the backend and UI Docker images via Buildx with GHA cache, saves them as a tarball artifact.
3. **e2e-compose** — loads those images, runs `docker compose up` (Kafka + Zookeeper + ClickHouse + backend + UI), then runs `e2e/run-e2e.sh` against the live stack: registers a topic, produces messages, waits for ingest, verifies JSON-path filtering, removes the topic.
4. **push-images** (main only) — tags and pushes both images to GHCR as `ghcr.io/<owner>/topicstore-backend:<sha>` and `:latest`.

### Running E2E locally against the live stack

```bash
docker compose up --build -d
./e2e/run-e2e.sh
```

The script registers a unique topic (e.g. `e2e.smoke.<unix-ts>`), produces 10 messages, polls `/api/lookup` until the count is reached, asserts the JSON-path filter works, then deletes the topic.

---

## Project layout

```
.
├── backend/                     # Micronaut + Kotlin
│   ├── Dockerfile
│   ├── build.gradle.kts
│   └── src/
│       ├── main/kotlin/co/codeyogi/topicstore/
│       │   ├── Application.kt
│       │   ├── api/             # REST controllers
│       │   ├── clickhouse/      # JDBC client + schema init
│       │   ├── kafka/           # ConsumerManager (dynamic subscriptions)
│       │   ├── model/
│       │   └── registry/        # TopicRegistry persisted in ClickHouse
│       └── test/kotlin/         # Testcontainers E2E
├── ui/                          # Kotlin/JS + React
│   ├── Dockerfile               # builds via Gradle, serves via nginx
│   ├── nginx.conf
│   ├── build.gradle.kts
│   └── src/jsMain/
│       ├── kotlin/co/codeyogi/topicstore/ui/
│       │   ├── App.kt
│       │   ├── Api.kt
│       │   ├── LookupPage.kt
│       │   ├── Main.kt
│       │   └── TopicsPage.kt
│       └── resources/index.html
├── e2e/run-e2e.sh               # black-box E2E against live compose stack
├── .github/workflows/ci.yml
├── docker-compose.yml
├── settings.gradle.kts
└── build.gradle.kts
```

---

## Operational notes

- **Backpressure / batching**: the backend polls Kafka in batches of up to 500 records or flushes every 1s, whichever comes first. Tune via `kafka.batch-size` and `kafka.batch-flush-ms`.
- **At-least-once delivery**: offsets are committed only after a successful ClickHouse batch insert. On crash, the last batch may be re-delivered; ClickHouse `MergeTree` accepts the duplicate. Add a dedup `ReplacingMergeTree` view downstream if exact-once matters for your case.
- **Schema**: payload is stored as raw `String`. Don't fight ClickHouse — query JSON with `JSONExtract*`. If a topic's schema stabilizes and you want typed columns, build a materialized view over `kafka_events` filtered by `topic`.
- **Auth**: not implemented. Front this with an authenticating proxy or add Micronaut Security before exposing externally.

---

## Troubleshooting

- **Backend keeps restarting** — ClickHouse not ready. The schema-init loop retries 30× / 2s. Check `docker compose logs clickhouse`.
- **Topic added but nothing lands** — confirm the topic actually exists on the broker and your producer is hitting the right `bootstrap.servers`. From inside the network: `kafka:9092`. From the host: `localhost:9094`.
- **`/api/health` says `kafka: false`** — broker advertised address mismatch. The compose file sets `KAFKA_CFG_ADVERTISED_LISTENERS=PLAINTEXT://kafka:9092,EXTERNAL://localhost:9094` — keep that in sync if you rename the service.
- **UI looks blank** — check the browser console; almost certainly an unproxied `/api` call (the nginx config in `ui/nginx.conf` proxies `/api/` → `backend:8080`).

---

## License

Internal/private. Add a license file before publishing.
