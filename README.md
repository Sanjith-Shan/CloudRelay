# CloudRelay

Real-time cloud gaming session orchestration microservice

![CI](https://github.com/Sanjith-Shan/CloudRelay/actions/workflows/ci.yml/badge.svg)
![Java](https://img.shields.io/badge/Java-17-orange)
![Spring Boot](https://img.shields.io/badge/Spring%20Boot-3.2-brightgreen)
![License](https://img.shields.io/badge/License-MIT-blue)

CloudRelay is a cloud native session management service built for real-time multiplayer gaming infrastructure. It handles the full session lifecycle from creation through matchmaking to termination, backed by Redis for sub-millisecond state caching and MongoDB for persistent session analytics. Designed for horizontal scalability with Kubernetes, Prometheus observability, and automated deployment via Helm.

## Architecture

CloudRelay uses a three tier architecture. REST controllers accept traffic, the service layer applies game session rules with a Redis cache-aside strategy, and MongoDB provides durable persistence with TTL based cleanup of expired sessions.

Session lifecycle events flow through a Redis pub/sub channel. Every replica subscribes to the channel and relays events to its own WebSocket clients, so a client connected to one instance still receives updates for actions handled by another instance behind the load balancer.

```
                       ┌─────────────────────────────────────────┐
   clients ──REST────▶ │  SessionController / MatchmakingController │
   clients ◀─STOMP──── │  WebSocket broker (/topic/session/{code})  │
                       └──────────────────┬──────────────────────┘
                                          │
                       ┌──────────────────▼──────────────────────┐
                       │   SessionService / MatchmakingService    │
                       │   metrics, validation, state machine     │
                       └───────┬──────────────────────┬──────────┘
                               │                      │
                  cache aside  │                      │  persistence
                ┌──────────────▼─────────┐  ┌─────────▼────────────┐
                │  Redis                 │  │  MongoDB             │
                │  session:{code} cache  │  │  sessions collection │
                │  matchqueue:{g}:{r}    │  │  TTL + unique index  │
                │  pub/sub event channel │  │  compound match index│
                └────────────────────────┘  └──────────────────────┘
```

The matchmaking queue is a Redis list per game and region. Players enqueue with LPUSH and the matcher dequeues the oldest waiting player with RPOP, then places them into an open session or creates a new one.

A deeper discussion of the design decisions lives in [ARCHITECTURE.md](ARCHITECTURE.md).

## Tech Stack

| Layer | Technology |
|---|---|
| Language | Java 17 |
| Framework | Spring Boot 3.2 (Web, WebSocket, Validation, Actuator) |
| Persistence | MongoDB 7 via Spring Data MongoDB |
| Cache and queue | Redis 7 via Spring Data Redis (Lettuce) |
| Metrics | Micrometer with Prometheus registry, Grafana dashboard |
| Packaging | Docker multi-stage build, Docker Compose |
| Orchestration | Kubernetes manifests, Helm 3 chart with HPA |
| CI | GitHub Actions (build, test, Docker smoke test, Helm lint) |
| Load testing | Python (stdlib only) |

## Quick Start

Requires Docker and Docker Compose. The stack includes the service, Redis, MongoDB, Prometheus, and Grafana.

```
cd docker
docker compose up -d --build
curl http://localhost:8080/actuator/health
```

| Service | URL |
|---|---|
| CloudRelay API | http://localhost:8080 |
| Prometheus | http://localhost:9090 |
| Grafana dashboard | http://localhost:3000 (admin / admin) |

Run the scripted demo to watch a full session lifecycle.

```
./scripts/demo.sh
```

## API Reference

Base path `/api/v1`

| Method | Path | Description |
|---|---|---|
| POST | `/sessions` | Create a session, returns 201 with Location header |
| GET | `/sessions/{code}` | Fetch a session by code (Redis cache first) |
| POST | `/sessions/{code}/join` | Join an open session |
| POST | `/sessions/{code}/leave?playerId=` | Leave a session, host is reassigned if needed |
| POST | `/sessions/{code}/start?playerId=` | Host starts the session |
| DELETE | `/sessions/{code}?playerId=` | Host terminates the session |
| GET | `/sessions?gameId=&region=` | List joinable sessions for a game and region |
| GET | `/sessions/player/{playerId}` | List live sessions for a player |
| POST | `/matchmaking/enqueue` | Enter the matchmaking queue, returns 202 |
| POST | `/matchmaking/match?gameId=&region=` | Match the oldest queued player |
| GET | `/matchmaking/queue?gameId=&region=` | Current queue depth |

Create a session

```
curl -X POST http://localhost:8080/api/v1/sessions \
  -H 'Content-Type: application/json' \
  -d '{"gameId":"cyberpunk-2077","playerId":"p1","displayName":"Alice","region":"us-west-2","maxPlayers":4,"minPlayersToStart":2}'
```

Join it

```
curl -X POST http://localhost:8080/api/v1/sessions/{code}/join \
  -H 'Content-Type: application/json' \
  -d '{"playerId":"p2","displayName":"Bob","region":"us-west-2"}'
```

Matchmake

```
curl -X POST http://localhost:8080/api/v1/matchmaking/enqueue \
  -H 'Content-Type: application/json' \
  -d '{"playerId":"p3","displayName":"Carol","gameId":"fortnite","region":"us-east-1"}'

curl -X POST 'http://localhost:8080/api/v1/matchmaking/match?gameId=fortnite&region=us-east-1'
```

Real-time updates are pushed over STOMP WebSocket. Connect to `/ws` and subscribe to `/topic/session/{code}` to receive SESSION_CREATED, PLAYER_JOINED, PLAYER_LEFT, SESSION_STARTED, and SESSION_TERMINATED events.

## Monitoring

Prometheus metrics are exposed at `/actuator/prometheus`. Custom metrics include

| Metric | Type | Meaning |
|---|---|---|
| `cloudrelay_sessions_created_total` | Counter | Sessions created since startup |
| `cloudrelay_sessions_active` | Gauge | Sessions currently in a live state |
| `cloudrelay_session_join_latency_seconds` | Timer | Join operation latency with histogram buckets |
| `cloudrelay_players_connected` | Gauge | Players across live sessions |

The bundled Grafana instance auto provisions a dashboard with active sessions, join latency percentiles (p50, p95, p99), request rate per endpoint, and JVM heap.

## Load Testing

A dependency free Python script drives concurrent create, join, read, and matchmaking traffic and reports latency percentiles per operation.

```
python3 scripts/loadtest.py --sessions 50 --concurrency 16
```

## Deployment

Plain manifests

```
kubectl apply -f k8s/
```

Helm with autoscaling (HPA from 2 to 8 replicas at 70 percent CPU) and an optional Prometheus ServiceMonitor

```
helm install cloudrelay helm/cloudrelay
```

Replicas coordinate through Redis, so WebSocket fanout and the matchmaking queue work unchanged at any replica count.

## Running Tests

Unit tests mock all external systems. The context load test expects Redis and MongoDB on localhost, matching the CI service containers.

```
cd docker && docker compose up -d redis mongo && cd ..
mvn clean test
```

## Project Structure

```
CloudRelay/
├── pom.xml
├── src/
│   ├── main/java/com/cloudrelay/
│   │   ├── config/          Redis template, pub/sub container, WebSocket broker
│   │   ├── controller/      REST endpoints
│   │   ├── dto/             Request and response payloads
│   │   ├── event/           Redis pub/sub publisher and subscriber
│   │   ├── exception/       Typed exceptions and global handler
│   │   ├── model/           GameSession, Player, SessionState
│   │   ├── repository/      Spring Data MongoDB repository
│   │   └── service/         Session lifecycle and matchmaking logic
│   └── test/java/com/cloudrelay/
├── docker/                  Dockerfile, compose stack, Prometheus, Grafana
├── k8s/                     Raw Kubernetes manifests
├── helm/cloudrelay/         Helm chart with HPA and ServiceMonitor
├── scripts/                 Demo walkthrough and Python load test
└── .github/workflows/       CI pipeline
```

## License

MIT, see [LICENSE](LICENSE).
