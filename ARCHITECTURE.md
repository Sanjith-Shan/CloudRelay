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

## Matchmaking

Two matchers ship, behind separate endpoints, because they make opposite trades and both are right somewhere.

### FIFO, the original

Each game and region pair gets its own Redis list, `matchqueue:{gameId}:{region}`. Enqueue is LPUSH, dequeue is RPOP, giving FIFO fairness per region. The matcher pops one player, looks for a WAITING session with room using the compound index, and either joins them in or creates a fresh session with that player as host.

Sharding the queue by game and region keeps lists short and naturally spreads load, and it mirrors how real platforms keep players near their game servers for latency. RPOP is atomic, so two matchers never pop the same player even across replicas.

Its virtue is that nobody ever waits: there is always a session to join or one to create. Its limit is that it has no opinion about who plays together, and on a busy queue that produces games nobody enjoys.

### Skill based, with an expanding window

The second matcher assembles a whole party at once from players of comparable rating.

Two questions have to be answered on every attempt, and they want different orderings of the same set. *Who has waited longest* decides who gets served next, because serving the queue in arrival order is what stops a player at an unpopular rating from being passed over indefinitely by newer arrivals who are easier to match. *Who is near this rating* decides who they play with. A queue sorted by arrival time answers the first in constant time and needs a full scan for the second; sorted by rating, the reverse.

So the index keeps both orderings and pays an extra O(log n) on write to keep them consistent:

```
skillq:{game}:{region}:rating    ZSET   member = playerId, score = rating
skillq:{game}:{region}:time      ZSET   member = playerId, score = enqueue millis
skillq:{game}:{region}:players   HASH   playerId -> the waiting player record
```

A Redis sorted set is a skip list with a hash index beside it, so the costs are the ones a skip list gives: O(log n) to add or remove, O(log n + k) for a range query with a LIMIT. The in-memory implementation used by the tests and the benchmark is a `ConcurrentSkipListSet`, which is the same structure, so the two agree operation for operation.

The player id is the member of both sorted sets and the record lives in a hash beside them. Putting the serialised record in the sorted set instead would make removal depend on Jackson producing byte-identical output for a value that has already been through a round trip, which is a fragile thing to build a queue on.

**The algorithm.** Take the longest waiting player as the anchor. Compute their acceptable skill band from how long they have waited. Pull the nearest candidates inside that band and take a party's worth. If there are not enough, form nothing and leave everyone queued — the anchor's window is wider on the next attempt, so the failure corrects itself rather than needing a different strategy.

Because the band is centred on the anchor's rating, "nearest the middle of the band" and "nearest the anchor" are the same ordering, and no second sort is needed.

**The window.** Width grows linearly with waiting, from 100 rating points to a cap of 600 over twenty seconds. Widening over time resolves a trade that cannot be resolved up front: a narrow window gives even games and long queues, a wide one fills lobbies instantly and produces mismatches, and which is correct depends on how many players happen to be queued near that rating right now — something not known when the player joins. A player in a dense part of the distribution matches almost immediately inside the narrow window; a player at the tail is not held forever, because their window grows until it reaches somebody. The system discovers the local density of the queue by waiting, which is cheaper and more robust than trying to model it.

Growth is linear rather than exponential because linear is predictable. A player can be told they will be matched within twenty seconds or the game is genuinely short of players, and that statement stays true.

**Finding the nearest candidates without sorting the band.** The obvious implementation collects everything in the band, sorts by distance, and takes the first few — O(k log k) in the size of the band, which is widest exactly when the queue is most congested. Instead both implementations walk outward from the midpoint, taking whichever neighbour is closer, and stop once they have enough. It is the merge step of a mergesort, and it costs O(log n) for the seek plus O(limit) for the walk regardless of how many players the window covers. Redis has no "nearest to a score" query, so the Redis version runs one descending and one ascending `ZRANGEBYSCORE`, each with a LIMIT, and merges them on the scores that come back with the members.

**What it deliberately does not do.** It assembles a lobby; it does not balance teams. Splitting a formed party into even sides is a partition problem and NP-hard in general, and mixing it in would make a fast, predictable step depend on a slow, approximate one.

Both matchers are measured against each other in `MatchmakingBenchmarkTest`, on speed and on match quality. The numbers are in [BENCHMARKS.md](BENCHMARKS.md).

## The analytics tee

Session lifecycle events go to two places with two different jobs.

Redis pub/sub fans each event out to every replica so that WebSocket clients stay in sync. That path is load bearing: a client that misses an event sees a stale lobby.

Kafka carries the same event to the lakehouse. That path is not load bearing and is engineered so that it can never behave as if it were. **A broker outage must never fail, slow, or block a game session**, and three things enforce it, each closing a gap the others leave open.

*The send is handed to a bounded executor.* `KafkaProducer.send` looks asynchronous and mostly is, but it blocks its caller for up to `max.block.ms` when cluster metadata is unavailable — which is exactly the situation during an outage. Off the request thread, that block costs a background thread instead of a player's response.

*The queue is bounded and overflow is discarded.* An unbounded queue in front of a broker that is down is just a slower way to run out of heap. A bounded queue with a discard policy turns a broker outage into a measurable gap in analytics data, which is the correct thing to lose.

*The drop is counted.* `cloudrelay_analytics_events_dropped_total` rising is the signal that the lakehouse is missing data. Silently dropping would leave it quietly incomplete with nothing to alert on, which is worse than the outage.

Producer settings follow from the same reasoning. `acks=1` with idempotence off, because leader acknowledgement is enough for analytics and waiting for the full ISR would add latency to protect data the pipeline can already survive losing. Turning idempotence off is not an oversight — the idempotent producer requires `acks=all`. The cost is that a retry can duplicate an event, and that is precisely the case the deduplication in silver absorbs: the duplicate is handled once, downstream, instead of being paid for on every send.

`AnalyticsTeeIT` drives a real producer through these exact settings against a broker that is not there. Two hundred publishes cost the caller under two seconds in total.

## The lakehouse

The tee feeds a medallion pipeline: raw events in, queryable aggregates out.

```
   SessionEventPublisher
        |            |
   Redis pub/sub   Kafka topic: cloudrelay.session-events
   (WebSocket)          |
                  Spark Structured Streaming
                        |
    Bronze (Delta) --> Silver (Delta) --> Gold (Delta)
    raw envelope       typed, validated   per-minute rollups
    ingest date        deduplicated       session durations
    partitioned        quarantine split   matchmaking funnel
                        |
                  OPTIMIZE + ZORDER, VACUUM
```

Spark lives in its own Maven module. It drags in roughly 300MB of transitive dependencies and has no business in the Spring Boot artifact that serves live traffic; two modules means two images and no shared classpath. That separation earned itself immediately — see the Jersey note under *What went wrong* below.

### Bronze: what arrived

Bronze appends the raw event envelope and never edits it. Nothing here parses the payload, because any parsing decision made at this layer cannot be revised later without going back to Kafka, which has a retention window. Silver can be dropped and rebuilt from bronze as often as the schema changes; bronze can only be rebuilt from a broker that may already have aged the data out.

The one thing it adds is provenance: ingest time and the source coordinates the row came from.

It is partitioned by the broker's append date rather than by a date parsed out of the payload. Reading the payload to decide the partition would make bronze depend on the schema it exists to be independent of, and a malformed event would then have nowhere to land.

### Silver: what is true

Silver parses against a declared schema, validates, deduplicates, and routes what fails.

**Parsing is against a contract, not a sample.** Spark's schema inference reads a sample of the data, so it is not a contract at all: the day a field arrives null in every sampled row, the column silently becomes a string and every downstream job changes shape. The schema is declared explicitly. Spark's PERMISSIVE mode then turns anything that does not fit into nulls rather than raising, which is why the first quality rule exists to catch exactly that.

**Bad rows are routed, not dropped and not fatal.** Dropping them makes the pipeline look healthy while it quietly loses data; failing the batch lets one malformed event stop every good one behind it. Quarantining keeps the bad row, the names of the rules it broke, and the bronze coordinates needed to replay it once the producer is fixed. Adding a rule is adding one entry to a list; the split, the failure list and the quarantine write all follow from it.

One rule points the other way from the rest. `schema_version_supported` quarantines events from a *newer* producer than this job understands. Forward compatibility in the safe direction: the rows are kept and replayable, and somebody is alerted on the deploy rather than on a silent column of nulls a week later.

### Deduplication: a watermark, and why it was removed

The documented way to deduplicate a stream is `withWatermark(...).dropDuplicatesWithinWatermark("event_id")`. Its state is bounded, it is what the Structured Streaming guide recommends, and it is what this pipeline did first.

It also silently lost data, and how is worth recording.

A watermark tracks the maximum event time seen and discards anything arriving more than the configured delay behind it. Under a small `maxFilesPerTrigger` — or any backfill, or any replay from bronze — a single early batch can contain events from across the whole time range. The watermark jumps almost to the maximum immediately, and from the next batch on, most events are older than it and are dropped as late. In this project's own compaction test, of 8,276 ingested events all but 354 vanished. Nothing failed. The counts were simply wrong.

The watermark is not the wrong tool because it is intolerant of late data. It is the wrong tool because **ordering is not a property this source has**. Kafka guarantees order within a partition, not across them, and replaying a Delta table guarantees nothing at all.

So the identity check moved from Spark's state store into the table. A MERGE with only a `WHEN NOT MATCHED THEN INSERT` clause is an insert-if-absent against silver's own `event_id`, which is correct regardless of arrival order, arbitrarily late data, batch replays after a crash, and producer retries — one mechanism for all four. It also leaves the query stateless, so a restart has nothing to recover beyond its offsets.

The cost is a join against silver on every micro-batch instead of a lookup in memory. That is paid down by pruning: silver is partitioned by `event_date` and the merge condition carries the batch's own date range, so Delta reads the partitions the batch could collide with rather than the table.

`ExactlyOnceRestartTest` pins all of it, including the case that motivated the change: duplicates arriving many batches apart are still deduplicated.

### Gold: what an operator asks

Four tables. Two are per-minute rollups computed from the stream; two are per-session and computed as batch recomputes over silver, because a session's duration is not known until it ends, which may be hours after the window its first event fell in.

The streaming rollups recompute rather than accumulate, and that is the design decision worth reading in this layer.

The obvious way to keep a rolling per-minute count is to MERGE each micro-batch's counts into the target with `SET count = target.count + source.count`. It is wrong in a way that only shows up under failure: `foreachBatch` guarantees at-least-once, so a driver that dies between the write and the offset commit re-runs the same batch, and an additive merge counts it twice. Delta's `txnAppId`/`txnVersion` markers exist to patch exactly that hole.

This pipeline takes the other road. Each batch works out which one-minute windows it touched, re-reads *those windows only* from silver, and MERGEs the recomputed value in as a replacement. Replacement is idempotent by construction — running the same batch twice produces the identical table — so correctness does not rest on a transaction marker being threaded through every write path. It also fixes late data for free: an event three minutes behind reopens its window and the number is simply right, where an additive pipeline would need a separate backfill to repair it.

What it costs is a bounded re-read of silver per batch, pruned to the touched partitions. That is the trade, and it is the right one: this data is worth more correct than cheap.

Quarantine keeps the `txnAppId`/`txnVersion` approach instead, because a malformed row has no reliable identity to match on.

Distinct counts in the streaming rollups use `approx_count_distinct`, which is HyperLogLog: a fixed-size sketch instead of holding every session code in the window in memory, for a few percent of error on a number that is read as a trend line. Where the number has to be exact, the batch tables compute it exactly.

### Compaction, and why a streaming table needs it

A streaming sink writes at least one file per micro-batch per partition, and a batch that carried forty rows still produces a file. Every file costs a metadata lookup, an open and a footer read before a single row comes back, so query time ends up governed by file count rather than by data volume — a table can get slower while staying the same size. This is the small-file problem, and streaming ingestion causes it by construction, not by misconfiguration.

`OPTIMIZE` rewrites those files into a few large ones and commits the swap atomically; readers see the old set until the commit and the new set after it, and nothing has to stop.

`ZORDER` goes further. Parquet keeps per-file min/max statistics, so a filter can skip a file whose range cannot contain a match. That only helps if related rows share files, and rows arrive ordered by time rather than by the columns anyone filters on. Z-ordering interleaves the bits of the chosen columns so that values close in several dimensions land close on disk, which is what turns those statistics into skipped files.

`VACUUM` is separate for a reason. OPTIMIZE only rewrites the log; the originals stay on disk so that readers mid-query keep working and time travel to earlier versions still resolves. Only VACUUM frees the space, and vacuuming below the retention window breaks both of those.

Measured numbers are in [BENCHMARKS.md](BENCHMARKS.md).

### Time travel

A Delta table is not a directory of files, it is an ordered log of commits, each recording which files were added and which removed. "The current table" is that log replayed to the end; "the table at version 7" is the same replay stopped early. Nothing is copied and nothing is snapshotted, so the cost of keeping history is the cost of not having deleted the old files yet.

The practical value is that a bad deploy which wrote wrong rows at 03:00 is recoverable by reading the version before it, rather than by restoring a backup. The limit is equally real: VACUUM deletes the files older versions point at, so history reaches back exactly as far as the retention window and not one commit further.

### The contract between the two modules

The service and the lakehouse do not depend on each other. Putting Spring Boot on Spark's test classpath is a dependency fight worth avoiding, and the lakehouse should build without the producer.

So the wire format is held by a pair of tests that share one literal field list: `AnalyticsWireFormatTest` in the service asserts what the serialiser actually produces, and `SessionEventSchemaTest` in the lakehouse asserts what Spark parses with. Rename a field on either side and that side's build fails, which is a great deal better than the column quietly becoming null in silver and nobody noticing until a dashboard is wrong.

That pair immediately earned itself too: it caught that the wire format carries `session.metadata`, a free-form per-game map that silver does not model. Typing an arbitrary map would mean either forcing every game into one shape or widening the column until it carried nothing. The field stays in bronze inside the raw payload, where a game-specific job can parse its own shape out of it, and the omission is now a listed decision rather than an accident.

## What went wrong, and what it cost

Three problems in this build were invisible until something specific was tried. They are recorded because each one is the kind that does not announce itself.

**Spring Boot's dependency management broke Spark, silently.** Boot 3.2 pins Jersey to 3.1.x, which is Jakarta EE 10 and lives in the `jakarta.servlet` namespace. Spark 3.5.1 is built against Jersey 2.40, which is `javax.servlet`. Nothing failed at compile time and nothing failed at resolution; `SparkContext` died on startup with *"Servlet class org.glassfish.jersey.servlet.ServletContainer is not a javax.servlet.Servlet"*, which names none of this. The fix is a `jersey-bom` import pinning 2.40 in the lakehouse module. That override is correct there and would be wrong in the service, which is the clearest possible argument for the two module split.

**The watermark lost 96% of the data without failing.** Covered above. The lesson generalises: a stateful operator whose correctness depends on approximate event-time ordering is a liability on any source that does not guarantee it, and the failure mode is wrong numbers rather than an exception.

**`Trigger.AvailableNow` ignores the rate limit on a Delta source.** It takes the whole backlog as one batch, which is usually what a catch-up run wants and is exactly wrong when the batch structure is the thing being studied. A replay that processes an hour of events in one enormous batch behaves nothing like the live pipeline that produced them: different memory profile, different file layout on the way out, and no way to reproduce the small-file problem a short trigger interval causes. Hence the `RATE_LIMITED` mode, which replays with the batch structure a live job would have had.

## Known limits and next steps

1. The matchmaker runs on demand through an endpoint. A scheduled worker with leader election, or a Redis Stream consumer group, would drain queues continuously instead of when something asks.
2. Skill ratings are supplied by the caller. A real system would compute them — Elo or Glicko updated from match outcomes — which means the lakehouse would feed the matchmaker rather than only observing it. That loop is the obvious next thing to build, and it is the point at which the two halves of this project stop being independent.
3. There is no authentication. A gateway issuing JWTs with player identity would slot in front without service changes.
4. MongoDB runs as a single node in the demo stack. Production would run a replica set, and at very large scale the document model maps cleanly onto Cassandra partitioned by session code.
5. The lakehouse runs Spark in local mode against a local filesystem. The code is unchanged on a cluster against object storage — that is what the Delta log is for — but "unchanged in principle" is not the same as measured, and it has not been measured.
6. Gold's streaming rollups re-read silver per batch. Bounded by partition pruning and correct, but at a much higher event rate the recompute window would need to be capped explicitly rather than derived from whatever the batch happened to touch.
