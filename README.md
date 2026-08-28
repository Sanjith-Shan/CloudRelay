# CloudRelay

Real-time cloud gaming session orchestration, and a streaming lakehouse over its event stream.

![CI](https://github.com/Sanjith-Shan/CloudRelay/actions/workflows/ci.yml/badge.svg)
![Java](https://img.shields.io/badge/Java-17-orange)
![Spring Boot](https://img.shields.io/badge/Spring%20Boot-3.2-brightgreen)
![Spark](https://img.shields.io/badge/Spark-3.5.1-e25a1c)
![Delta Lake](https://img.shields.io/badge/Delta%20Lake-3.2-00ADD4)
![License](https://img.shields.io/badge/License-MIT-blue)

CloudRelay is two things that fit together.

**A session service.** Spring Boot over MongoDB and Redis, handling the full lifecycle of a multiplayer game session — create, matchmake, join, start, terminate — with sub-millisecond cached reads, optimistic concurrency on contended writes, WebSocket fanout across replicas, and Prometheus metrics. It runs on Kubernetes with an HPA.

**A lakehouse over what that service does.** Every lifecycle event is teed onto Kafka and consumed by a Spark Structured Streaming pipeline that lands it in Delta Lake through bronze, silver and gold, with exactly-once ingestion, a quarantine path for malformed events, and compaction that is measured rather than assumed.

The design decisions and their trade-offs are in [ARCHITECTURE.md](ARCHITECTURE.md). Every performance number, with the machine it was measured on, is in [BENCHMARKS.md](BENCHMARKS.md).

## Architecture

```
                       ┌──────────────────────────────────────────────┐
   clients ──REST────▶ │ SessionController / MatchmakingController     │
   clients ◀─STOMP──── │ WebSocket broker (/topic/session/{code})       │
                       └───────────────────┬──────────────────────────┘
                                           │
                       ┌───────────────────▼──────────────────────────┐
                       │ SessionService  ·  MatchmakingService         │
                       │ SkillMatchmakingService                       │
                       └──┬──────────────┬───────────────┬────────────┘
            cache aside   │              │ persistence   │ every lifecycle event
                ┌─────────▼──────┐  ┌────▼──────────┐    │
                │ Redis          │  │ MongoDB       │    │
                │ session cache  │  │ sessions      │    │
                │ match queues   │  │ TTL + indexes │    │
                │ skill index    │  └───────────────┘    │
                │ pub/sub        │                       │
                └────────────────┘        ┌──────────────▼──────────────┐
                                          │ SessionEventPublisher        │
                                          │  ├─ Redis pub/sub → WebSocket│
                                          │  └─ Kafka tee → lakehouse    │
                                          └──────────────┬──────────────┘
                                                         │
                                          ┌──────────────▼──────────────┐
                                          │ Spark Structured Streaming   │
                                          │                              │
                                          │  Bronze → Silver → Gold      │
                                          │  raw      typed    rollups   │
                                          │           deduped  durations │
                                          │           quarantine funnel  │
                                          │                              │
                                          │  OPTIMIZE · ZORDER · VACUUM  │
                                          └──────────────────────────────┘
```

The Kafka tee is deliberately not load bearing. A broker outage degrades analytics and never touches a game session — measured at 200 publishes against a dead broker costing the caller under two seconds in total.

## What is worth looking at

If you are reading this to judge the engineering rather than to run it:

| Where | Why |
|---|---|
| [`SilverTransform`](cloudrelay-lakehouse/src/main/java/com/cloudrelay/lakehouse/transform/SilverTransform.java) | Deduplication started as a watermark, silently lost 96% of a backfill, and became a Delta MERGE. The class comment explains why the watermark was the wrong tool. |
| [`GoldAggregates`](cloudrelay-lakehouse/src/main/java/com/cloudrelay/lakehouse/aggregate/GoldAggregates.java) | Streaming rollups that recompute rather than accumulate, so idempotency does not depend on transaction markers. |
| [`SkillMatchmaker`](cloudrelay-service/src/main/java/com/cloudrelay/matchmaking/SkillMatchmaker.java) | Expanding-window matchmaking over a two-ordering skip-list index. O(log n + p) against a linear scan's O(n log n). |
| [`KafkaAnalyticsSink`](cloudrelay-service/src/main/java/com/cloudrelay/analytics/KafkaAnalyticsSink.java) | The three mechanisms that stop a broker outage from reaching a player. |
| [`ExactlyOnceRestartTest`](cloudrelay-lakehouse/src/test/java/com/cloudrelay/lakehouse/ExactlyOnceRestartTest.java) | Kills the pipeline mid-stream and proves nothing is lost or duplicated. |
| [ARCHITECTURE.md § What went wrong](ARCHITECTURE.md#what-went-wrong-and-what-it-cost) | Three bugs that produced wrong answers instead of errors, and what each cost. |

## Quick start

Requires Docker and Docker Compose.

```bash
cd docker
docker compose up -d --build
curl http://localhost:8080/actuator/health
```

That brings up the service, Redis, MongoDB, Kafka, Prometheus and Grafana, with the analytics tee publishing to Kafka.

| Service | URL |
|---|---|
| CloudRelay API | http://localhost:8080 |
| Prometheus | http://localhost:9090 |
| Grafana dashboard | http://localhost:3000 (admin / admin) |
| Kafka | localhost:9092 |

Add the Spark pipeline. It is behind a compose profile because its image carries Spark and takes a few minutes to build the first time.

```bash
docker compose --profile lakehouse up -d --build
docker compose logs -f lakehouse
```

Then walk a full session lifecycle, including both matchmakers:

```bash
./scripts/demo.sh
```

## Running the lakehouse locally

Without Docker, against whatever Kafka you have:

```bash
./scripts/lakehouse.sh run          # bronze → silver → gold, then stop
./scripts/lakehouse.sh query        # the analytics pack over gold
./scripts/lakehouse.sh compact      # OPTIMIZE, with before and after numbers
./scripts/lakehouse.sh history      # Delta version history with row counts
```

Or with no broker at all, replaying generated events from disk:

```bash
python3 scripts/generate_events.py --sessions 5000 --files 50
./scripts/lakehouse.sh run --source file
./scripts/lakehouse.sh query
```

Useful flags: `--continuous` runs the long-lived daemon a deployment would; `--rate-limited --max-files-per-trigger 1` replays a backlog with the batch structure a live job would have had, which is how the small-file problem is reproduced on demand.

**Events that really came through the service** reach the lakehouse the production way: start the compose stack and drive it with the load generator.

```bash
python3 scripts/loadtest.py --sessions 200 --concurrency 16
./scripts/lakehouse.sh query
```

## API reference

Base path `/api/v1`.

| Method | Path | Description |
|---|---|---|
| POST | `/sessions` | Create a session, returns 201 with a Location header |
| GET | `/sessions/{code}` | Fetch a session by code (Redis cache first) |
| POST | `/sessions/{code}/join` | Join an open session |
| POST | `/sessions/{code}/leave?playerId=` | Leave; the host is reassigned if needed |
| POST | `/sessions/{code}/start?playerId=` | Host starts the session |
| DELETE | `/sessions/{code}?playerId=` | Host terminates the session |
| GET | `/sessions?gameId=&region=` | List joinable sessions |
| GET | `/sessions/player/{playerId}` | List a player's live sessions |
| POST | `/matchmaking/enqueue` | FIFO queue, returns 202 |
| POST | `/matchmaking/match?gameId=&region=` | Match the oldest queued player |
| GET | `/matchmaking/queue?gameId=&region=` | FIFO queue depth |
| POST | `/matchmaking/skill/enqueue` | Skill queue, accepts a `skillRating` |
| POST | `/matchmaking/skill/match?gameId=&region=&partySize=` | Assemble a party, 204 if not enough compatible players yet |
| GET | `/matchmaking/skill/queue?gameId=&region=` | Skill queue depth and the current window policy |

Create a session:

```bash
curl -X POST http://localhost:8080/api/v1/sessions \
  -H 'Content-Type: application/json' \
  -d '{"gameId":"cyberpunk-2077","playerId":"p1","displayName":"Alice","region":"us-west-2","maxPlayers":4,"minPlayersToStart":2}'
```

Skill based matchmaking. A 204 means there are not yet enough compatible players; the waiting players' search windows widen and the next attempt is likelier to succeed.

```bash
curl -X POST http://localhost:8080/api/v1/matchmaking/skill/enqueue \
  -H 'Content-Type: application/json' \
  -d '{"playerId":"p3","displayName":"Carol","gameId":"fortnite","region":"us-east-1","skillRating":1450}'

curl -X POST 'http://localhost:8080/api/v1/matchmaking/skill/match?gameId=fortnite&region=us-east-1&partySize=2'
```

Real-time updates are pushed over STOMP. Connect to `/ws` and subscribe to `/topic/session/{code}` for SESSION_CREATED, PLAYER_JOINED, PLAYER_LEFT, SESSION_STARTED and SESSION_TERMINATED.

## The lakehouse tables

| Table | Layer | What it holds |
|---|---|---|
| `bronze_session_events` | Bronze | Raw event envelopes with ingest time and source offsets, partitioned by ingest date |
| `silver_session_events` | Silver | Typed, validated, deduplicated events, partitioned by event date |
| `silver_quarantine` | Silver | Rejected rows with the names of the rules they broke, kept for replay |
| `gold_session_lifecycle_1m` | Gold | Per minute, per game, per region: creations, starts, terminations, joins, leaves |
| `gold_region_concurrency_1m` | Gold | Per minute, per region: active sessions and peak session size |
| `gold_session_duration` | Gold | One row per session: duration, peak players, whether it ever started |
| `gold_matchmaking_funnel` | Gold | Created → started conversion and duration percentiles per game and region |

`./scripts/lakehouse.sh query` runs a pack of operator questions over these, including which sessions were abandoned before they ever started and what the quarantine is rejecting.

## Monitoring

Prometheus scrapes `/actuator/prometheus`. Beyond the JVM and HTTP defaults:

| Metric | Type | Meaning |
|---|---|---|
| `cloudrelay_sessions_created_total` | Counter | Sessions created since startup |
| `cloudrelay_sessions_active` | Gauge | Sessions currently in a live state |
| `cloudrelay_session_join_latency_seconds` | Timer | Join latency, with histogram buckets for server-side percentiles |
| `cloudrelay_players_connected` | Gauge | Players across live sessions |
| `cloudrelay_matchmaking_rating_spread` | Summary | Rating gap within a formed party — match quality |
| `cloudrelay_matchmaking_wait_seconds` | Summary | How long the anchor player waited — the price of that quality |
| `cloudrelay_analytics_events_published_total` | Counter | Events accepted by the broker |
| `cloudrelay_analytics_events_dropped_total` | Counter | Events shed by backpressure; the signal the lakehouse is missing data |

Rating spread and wait time are published together on purpose. A window change moves both, and watching one without the other is how a matchmaker gets tuned into producing fast, terrible games.

## Tests

154 tests across 22 classes: 96 in the service, 58 in the lakehouse. Ten of them
need Docker; the rest do not.

```bash
mvn verify                       # everything, including Testcontainers Kafka
mvn verify -DskipITs             # no Docker needed
mvn verify -DskipUTs=true        # only the Testcontainers suites
mvn -pl cloudrelay-lakehouse verify -DskipITs    # the Spark suites alone
```

`AnalyticsWireFormatTest` and `SessionEventSchemaTest` are two halves of one contract test, one in each module. They share a literal field list, so renaming a field on either side fails that side's build rather than turning a silver column quietly into nulls.

The service's context test expects Redis and MongoDB on localhost, matching the CI service containers:

```bash
cd docker && docker compose up -d redis mongo && cd ..
mvn verify -DskipITs
```

The lakehouse suites run a real local Spark and write real Delta tables; no mocks, no fixtures pretending to be a table. Notable ones:

- `ExactlyOnceRestartTest` — kills bronze and silver mid-stream and proves nothing is lost or duplicated, and that duplicates arriving many batches apart are still absorbed
- `DataQualityTest` — seven ways an event can be malformed, each landing in quarantine with the rule that rejected it
- `CompactionBenchmarkTest` — fragments a table on purpose, compacts it, and asserts on the file counts from the transaction log
- `AnalyticsQueriesTest` — runs the whole operator SQL pack against a pipeline that has never rejected anything, because a query pack that falls over when nothing is wrong is unavailable exactly when somebody is checking that nothing is wrong
- `MatchmakingBenchmarkTest` — re-measures the matchmaking algorithm on every run and fails if the index ever stops outscaling the linear scan
- `InMemoryWaitingPlayerIndexTest` — checks the skip-list index against a deliberately naive linear scan on randomised queues, so the fast implementation has an oracle rather than only a benchmark

**On Docker Engine 29 or newer**, Testcontainers negotiates an API version the daemon has dropped. The build pins a compatible one; override with `-Ddocker.api.version=` if a particular machine needs something else.

## Deployment

```bash
kubectl apply -f k8s/
helm install cloudrelay helm/cloudrelay     # HPA, 2 to 8 replicas at 70% CPU
```

Replicas coordinate through Redis, so WebSocket fanout, both match queues and the skill index all work unchanged at any replica count.

## Project structure

```
CloudRelay/
├── pom.xml                        parent: shared versions, Spark and Delta pinning
├── cloudrelay-service/            the Spring Boot service
│   └── src/main/java/com/cloudrelay/
│       ├── analytics/             the Kafka tee and its failure isolation
│       ├── config/                Redis, WebSocket, Kafka, matchmaking wiring
│       ├── controller/            REST endpoints
│       ├── event/                 lifecycle event envelope and fanout
│       ├── matchmaking/           skill index, expanding window, party formation
│       ├── model/ dto/ repository/
│       └── service/               session lifecycle and both matchers
├── cloudrelay-lakehouse/          the Spark pipeline
│   └── src/main/java/com/cloudrelay/lakehouse/
│       ├── ingest/                event sources and bronze
│       ├── transform/             silver, data quality expectations
│       ├── aggregate/             gold rollups
│       ├── maintenance/           OPTIMIZE, VACUUM, the compaction benchmark
│       └── query/                 the analytics pack and time travel
├── docker/                        two images, compose stack with Kafka
├── k8s/  helm/                    manifests and chart
├── scripts/                       demo, load test, pipeline runner, event generator
└── .github/workflows/             six CI jobs
```

## License

MIT, see [LICENSE](LICENSE).
