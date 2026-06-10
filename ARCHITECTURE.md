# CloudRelay Architecture Notes

This document explains the design decisions behind CloudRelay and the trade-offs they carry. It is written for engineers evaluating the system, not as user documentation.

## Why a session orchestrator

Cloud gaming platforms hold a large amount of short lived, frequently mutated state. A session moves through a small state machine (WAITING, STARTING, ACTIVE, PAUSED, TERMINATED) while players join and leave, and thousands of these transitions happen per second at platform scale. The interesting engineering problems are read latency, write contention, horizontal scale, and observability. CloudRelay is built around those four problems.

## Data layout

MongoDB is the system of record. Each session is a single document, with players embedded rather than referenced. Embedding keeps every lifecycle operation a one document read and a one document write, which avoids cross document transactions entirely.

Three indexes carry the workload.

| Index | Purpose |
|---|---|
| Unique index on `sessionCode` | Constant time lookup by the code players share |
| Compound index on `gameId, region, state` | The matchmaking query shape |
| TTL index on `expiresAt` | MongoDB deletes abandoned sessions on its own |

The TTL index doubles as garbage collection. A WAITING session expires two hours after creation. Starting a session pushes `expiresAt` out by six more hours, so live games are never reaped mid play.

A NoSQL document store fits this data because the schema is fluid (per game metadata rides in a free form map), there are no relational joins, and the access pattern is almost entirely point reads and point writes by key. The same model would port naturally to Cassandra with `sessionCode` as the partition key, which is the kind of store this design anticipates at larger scale.

## Caching strategy

Reads hit Redis first under the key `session:{code}` with a thirty minute TTL, falling back to MongoDB and repopulating the cache on a miss. Every successful write refreshes the cache, and termination evicts it. This is classic cache-aside.

Two deliberate choices here.

1. Redis failures degrade rather than break. Every cache call is wrapped so a Redis outage turns into slower MongoDB backed reads instead of errors.
2. List queries are never cached. Session lists for a game and region change with every join and leave, so a cached list is stale the moment it is written. Point lookups dominate traffic anyway.

## Write contention

Two players can join the same nearly full session through different replicas at the same moment. A naive read modify write would let both in and overshoot `maxPlayers`. CloudRelay uses a `@Version` field on the session document, so Spring Data issues a compare and swap on every save. The losing writer gets an optimistic locking failure, the join path evicts its stale cache entry, rereads, and retries up to three times before giving up with a clear error.

Optimistic locking was chosen over a distributed lock because contention is rare (most sessions see a handful of joins) and lock free reads keep the hot path fast. A Redis based lock would add a network round trip to every join to protect against a conflict that almost never happens.

## Event fanout across replicas

WebSocket connections are sticky to one instance, but writes land on any instance behind the load balancer. If each instance broadcast only to its own STOMP clients, a player connected to replica A would miss every event produced by replica B.

CloudRelay publishes every lifecycle event to a single Redis pub/sub channel, `cloudrelay:session-events`. Every replica subscribes, and each one relays incoming events to its local broker destination `/topic/session/{code}`. The result is that the WebSocket layer scales horizontally with zero coordination beyond Redis.

Redis pub/sub is fire and forget, which is acceptable for UI refresh events because the REST response already carries authoritative state. If events needed durability or replay, Redis Streams or Kafka with consumer groups would replace the channel without touching the service layer, since publishing is isolated behind `SessionEventPublisher`.

## Matchmaking queue

Each game and region pair gets its own Redis list, `matchqueue:{gameId}:{region}`. Enqueue is LPUSH, dequeue is RPOP, giving FIFO fairness per region. The matcher pops one player, looks for a WAITING session with room using the compound index, and either joins them in or creates a fresh session with that player as host.

Sharding the queue by game and region keeps lists short and naturally spreads load, and it mirrors how real platforms keep players near their game servers for latency. RPOP is atomic, so two matchers never pop the same player even across replicas.

## Observability

Micrometer publishes to Prometheus through the actuator endpoint. Beyond the JVM and HTTP defaults, four domain metrics describe the health of the business itself.

| Metric | Why it matters |
|---|---|
| `cloudrelay_sessions_created_total` | Demand signal and rate alerting |
| `cloudrelay_sessions_active` | Capacity planning input |
| `cloudrelay_session_join_latency_seconds` | The user facing hot path, with histogram buckets for percentile queries |
| `cloudrelay_players_connected` | Concurrency watermark |

Join latency publishes full histogram buckets so Prometheus can compute p50, p95, and p99 server side. The bundled Grafana dashboard charts exactly those percentiles, because averages hide tail latency and tail latency is what players feel.

Kubernetes probes use the actuator liveness and readiness groups, so a wedged instance is restarted and a starting instance receives no traffic until its Mongo and Redis connections are ready.

## Deployment model

The service is stateless. All state lives in Redis and MongoDB, which is what makes the HPA in the Helm chart safe. Scaling from two to eight replicas requires no warmup, no session draining, and no rebalancing, because any replica can serve any request and the pub/sub channel keeps WebSocket clients consistent.

The CI pipeline mirrors production shape. Unit and context tests run against real Redis and MongoDB service containers, the Docker job boots the full compose stack and asserts on the health endpoint, and the Helm job lints and renders the chart.

## Known limits and next steps

1. The matchmaker currently runs on demand through an endpoint. A scheduled worker with leader election (or a Redis Stream consumer group) would drain queues continuously.
2. Redis pub/sub delivers at most once. UI events tolerate that, but an activity feed or analytics pipeline should move to Redis Streams or Kafka.
3. There is no authentication. A gateway issuing JWTs with player identity would slot in front without service changes.
4. MongoDB runs as a single node in the demo stack. Production would run a replica set, and at very large scale the document model maps cleanly onto Cassandra partitioned by session code.
5. Skill based matchmaking would replace the FIFO list with a Redis sorted set scored by rating, matching players within a widening window.
