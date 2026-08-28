#!/usr/bin/env python3
"""Write synthetic CloudRelay session events into the lakehouse landing directory.

These are SYNTHETIC events, not traffic from the running service. They exist so
the pipeline can be driven at a volume that makes compaction measurable without
holding an API under load for an hour, and any number measured from them has to
say so.

For events that really came through the service, start the compose stack with
the analytics tee enabled and drive it with scripts/loadtest.py; those reach the
lakehouse through Kafka, the way production events would.

Usage
    python3 scripts/generate_events.py --sessions 20000 --files 200
    python3 scripts/generate_events.py --sessions 500 --out data/lakehouse/landing
"""

import argparse
import json
import os
import random
import uuid
from datetime import datetime, timedelta, timezone

GAMES = ["cyberpunk-2077", "fortnite", "rocket-league", "apex-legends"]
REGIONS = ["us-west-2", "us-east-1", "eu-central-1", "ap-northeast-1"]
SCHEMA_VERSION = 1


def iso(moment):
    """ISO-8601 at millisecond precision.

    The service truncates to milliseconds for a reason worth repeating here:
    Spark's string-to-timestamp cast returns null past six fractional digits,
    and a null event_time is a quarantined event rather than a loud failure.
    Generated events have to follow the same rule or they exercise the
    quarantine path instead of the happy one.
    """
    text = moment.astimezone(timezone.utc).isoformat(timespec="milliseconds")
    return text.replace("+00:00", "Z")


def player(session_code, index, region, joined_at):
    return {
        "playerId": f"{session_code}-p{index}",
        "displayName": f"Player {index}",
        "region": region,
        "joinedAt": iso(joined_at),
        "connected": True,
    }


def event(event_type, session_code, game_id, region, state,
          players, max_players, created_at, event_time, uptime_seconds):
    return {
        "eventId": str(uuid.uuid4()),
        "eventTime": iso(event_time),
        "type": event_type,
        "sessionCode": session_code,
        "schemaVersion": SCHEMA_VERSION,
        "session": {
            "id": f"{session_code}-id",
            "sessionCode": session_code,
            "gameId": game_id,
            "region": region,
            "state": state,
            "host": players[0],
            "players": players,
            "maxPlayers": max_players,
            "minPlayersToStart": 2,
            "createdAt": iso(created_at),
            "updatedAt": iso(event_time),
            "expiresAt": iso(created_at + timedelta(hours=2)),
            "currentPlayerCount": len(players),
            "isFull": len(players) >= max_players,
            "uptimeSeconds": int(uptime_seconds),
        },
    }


def session_lifecycle(index, start, rng, spread_seconds):
    """One session's worth of events: created, joins, maybe started, terminated.

    Generating lifecycles rather than unrelated rows matters. The gold tables
    measure session duration and a created-to-started funnel, and neither means
    anything over events that never form a session.
    """
    code = f"S{index:07d}"
    game = rng.choice(GAMES)
    region = rng.choice(REGIONS)
    max_players = 4
    created_at = start + timedelta(seconds=rng.randint(0, spread_seconds))

    players = [player(code, 0, region, created_at)]
    events = [event("SESSION_CREATED", code, game, region, "WAITING",
                    list(players), max_players, created_at, created_at, 0)]

    at = created_at
    for i in range(1, rng.randint(1, max_players)):
        at += timedelta(seconds=rng.randint(1, 20))
        players.append(player(code, i, region, at))
        state = "STARTING" if len(players) >= 2 else "WAITING"
        events.append(event("PLAYER_JOINED", code, game, region, state,
                            list(players), max_players, created_at, at,
                            (at - created_at).total_seconds()))

    # Four in five sessions get going; the rest are the abandoned lobbies the
    # funnel table exists to count.
    started = len(players) >= 2 and rng.randint(0, 4) > 0
    if started:
        at += timedelta(seconds=rng.randint(1, 30))
        events.append(event("SESSION_STARTED", code, game, region, "ACTIVE",
                            list(players), max_players, created_at, at,
                            (at - created_at).total_seconds()))
        at += timedelta(seconds=rng.randint(60, 1800))
    else:
        at += timedelta(seconds=rng.randint(30, 120))

    events.append(event("SESSION_TERMINATED", code, game, region, "TERMINATED",
                        list(players), max_players, created_at, at,
                        (at - created_at).total_seconds()))
    return events


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sessions", type=int, default=5000)
    parser.add_argument("--files", type=int, default=50,
                        help="how many files to split the events across; more "
                             "files means more micro-batches and a more "
                             "fragmented table")
    parser.add_argument("--out", default="data/lakehouse/landing")
    parser.add_argument("--spread-minutes", type=int, default=60,
                        help="how wide a time range the events cover")
    parser.add_argument("--seed", type=int, default=20260827)
    args = parser.parse_args()

    rng = random.Random(args.seed)
    start = datetime.now(timezone.utc) - timedelta(minutes=args.spread_minutes)

    lines = []
    for i in range(args.sessions):
        for e in session_lifecycle(i, start, rng, args.spread_minutes * 60):
            lines.append(json.dumps(e, separators=(",", ":")))

    os.makedirs(args.out, exist_ok=True)
    per_file = max(1, -(-len(lines) // args.files))
    written = 0
    for index, offset in enumerate(range(0, len(lines), per_file)):
        path = os.path.join(args.out, f"events-{index:05d}.json")
        with open(path, "w", encoding="utf-8") as handle:
            handle.write("\n".join(lines[offset:offset + per_file]))
            handle.write("\n")
        written += 1

    print(f"{len(lines):,} synthetic events from {args.sessions:,} sessions")
    print(f"written to {args.out} across {written} files")
    print("run the pipeline with: ./scripts/lakehouse.sh run --source file")


if __name__ == "__main__":
    main()
