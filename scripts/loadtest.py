#!/usr/bin/env python3
"""Load test for the CloudRelay API.

Drives concurrent session lifecycle and matchmaking traffic using only the
Python standard library, then reports latency percentiles per operation.

Usage
    python3 scripts/loadtest.py --sessions 50 --concurrency 16
"""

import argparse
import json
import random
import string
import sys
import time
import urllib.error
import urllib.request
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

REGIONS = ["us-west-2", "us-east-1", "eu-central-1", "ap-northeast-1"]
GAMES = ["cyberpunk-2077", "fortnite", "rocket-league", "apex-legends"]


class Stats:
    """Thread safe enough for CPython appends, collects latency per op."""

    def __init__(self):
        self.latencies = defaultdict(list)
        self.errors = defaultdict(int)

    def record(self, op, seconds):
        self.latencies[op].append(seconds * 1000.0)

    def error(self, op):
        self.errors[op] += 1


def request(base_url, method, path, body=None):
    url = base_url + path
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method)
    if data is not None:
        req.add_header("Content-Type", "application/json")
    with urllib.request.urlopen(req, timeout=10) as resp:
        raw = resp.read()
        return json.loads(raw) if raw else None


def timed(stats, op, base_url, method, path, body=None):
    start = time.perf_counter()
    try:
        result = request(base_url, method, path, body)
        stats.record(op, time.perf_counter() - start)
        return result
    except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError):
        stats.error(op)
        return None


def rand_id(prefix):
    suffix = "".join(random.choices(string.ascii_lowercase + string.digits, k=8))
    return f"{prefix}-{suffix}"


def run_lifecycle(stats, base_url, joins_per_session):
    """One full session story. Create, read, fill with players, start, terminate."""
    game = random.choice(GAMES)
    region = random.choice(REGIONS)
    host_id = rand_id("host")

    created = timed(stats, "create", base_url, "POST", "/api/v1/sessions", {
        "gameId": game,
        "playerId": host_id,
        "displayName": "Load Host",
        "region": region,
        "maxPlayers": max(2, joins_per_session + 1),
        "minPlayersToStart": 2,
    })
    if not created:
        return
    code = created["sessionCode"]

    timed(stats, "get", base_url, "GET", f"/api/v1/sessions/{code}")

    for _ in range(joins_per_session):
        timed(stats, "join", base_url, "POST", f"/api/v1/sessions/{code}/join", {
            "playerId": rand_id("player"),
            "displayName": "Load Player",
            "region": region,
        })

    for _ in range(3):
        timed(stats, "get", base_url, "GET", f"/api/v1/sessions/{code}")

    timed(stats, "start", base_url, "POST",
          f"/api/v1/sessions/{code}/start?playerId={host_id}")
    timed(stats, "terminate", base_url, "DELETE",
          f"/api/v1/sessions/{code}?playerId={host_id}")


def run_matchmaking(stats, base_url):
    """Enqueue a player then run the matcher for that game and region."""
    game = random.choice(GAMES)
    region = random.choice(REGIONS)

    timed(stats, "enqueue", base_url, "POST", "/api/v1/matchmaking/enqueue", {
        "playerId": rand_id("seeker"),
        "displayName": "Load Seeker",
        "gameId": game,
        "region": region,
    })
    timed(stats, "match", base_url, "POST",
          f"/api/v1/matchmaking/match?gameId={game}&region={region}")


def percentile(sorted_values, pct):
    if not sorted_values:
        return 0.0
    index = min(len(sorted_values) - 1, int(round(pct / 100.0 * len(sorted_values) + 0.5)) - 1)
    return sorted_values[max(0, index)]


def report(stats, wall_seconds):
    total = sum(len(v) for v in stats.latencies.values())
    print(f"\n{total} requests in {wall_seconds:.1f}s "
          f"({total / wall_seconds:.0f} req/s)\n")
    header = f"{'operation':<12}{'count':>7}{'p50 ms':>10}{'p95 ms':>10}{'p99 ms':>10}{'max ms':>10}{'errors':>8}"
    print(header)
    print("-" * len(header))
    for op in sorted(stats.latencies):
        values = sorted(stats.latencies[op])
        print(f"{op:<12}{len(values):>7}"
              f"{percentile(values, 50):>10.1f}"
              f"{percentile(values, 95):>10.1f}"
              f"{percentile(values, 99):>10.1f}"
              f"{values[-1]:>10.1f}"
              f"{stats.errors.get(op, 0):>8}")
    for op, count in stats.errors.items():
        if op not in stats.latencies:
            print(f"{op:<12}{0:>7}{'':>40}{count:>8}")


def main():
    parser = argparse.ArgumentParser(description="CloudRelay load test")
    parser.add_argument("--base-url", default="http://localhost:8080")
    parser.add_argument("--sessions", type=int, default=50,
                        help="number of full session lifecycles to run")
    parser.add_argument("--joins-per-session", type=int, default=3)
    parser.add_argument("--matchmaking", type=int, default=30,
                        help="number of enqueue plus match cycles")
    parser.add_argument("--concurrency", type=int, default=16)
    args = parser.parse_args()

    try:
        request(args.base_url, "GET", "/actuator/health")
    except Exception:
        print(f"CloudRelay is not reachable at {args.base_url}", file=sys.stderr)
        print("Start it with cd docker && docker compose up -d", file=sys.stderr)
        sys.exit(1)

    stats = Stats()
    start = time.perf_counter()
    with ThreadPoolExecutor(max_workers=args.concurrency) as pool:
        futures = [pool.submit(run_lifecycle, stats, args.base_url, args.joins_per_session)
                   for _ in range(args.sessions)]
        futures += [pool.submit(run_matchmaking, stats, args.base_url)
                    for _ in range(args.matchmaking)]
        for future in as_completed(futures):
            future.result()
    report(stats, time.perf_counter() - start)


if __name__ == "__main__":
    main()
