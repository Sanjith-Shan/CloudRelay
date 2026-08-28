#!/usr/bin/env bash
# Walks through a full CloudRelay session lifecycle against a running stack.
# Start the stack first with cd docker && docker compose up -d --build
set -euo pipefail

BASE_URL="${BASE_URL:-http://localhost:8080}"

step() {
  printf '\n\033[1;36m== %s ==\033[0m\n' "$1"
}

pretty() {
  python3 -m json.tool 2>/dev/null || cat
}

step "Health check"
curl -sf "$BASE_URL/actuator/health" | pretty

step "Create a session (Alice hosts cyberpunk-2077 in us-west-2)"
CREATE=$(curl -s -X POST "$BASE_URL/api/v1/sessions" \
  -H 'Content-Type: application/json' \
  -d '{"gameId":"cyberpunk-2077","playerId":"alice","displayName":"Alice","region":"us-west-2","maxPlayers":4,"minPlayersToStart":2}')
echo "$CREATE" | pretty
CODE=$(echo "$CREATE" | python3 -c 'import sys,json;print(json.load(sys.stdin)["sessionCode"])')
echo "session code is $CODE"

step "Read it back (served from the Redis cache)"
curl -s "$BASE_URL/api/v1/sessions/$CODE" | pretty

step "Bob joins, session transitions WAITING to STARTING"
curl -s -X POST "$BASE_URL/api/v1/sessions/$CODE/join" \
  -H 'Content-Type: application/json' \
  -d '{"playerId":"bob","displayName":"Bob","region":"us-west-2"}' | pretty

step "Alice starts the game, session goes ACTIVE"
curl -s -X POST "$BASE_URL/api/v1/sessions/$CODE/start?playerId=alice" | pretty

step "Carol enters matchmaking for fortnite in us-east-1"
curl -s -X POST "$BASE_URL/api/v1/matchmaking/enqueue" \
  -H 'Content-Type: application/json' \
  -d '{"playerId":"carol","displayName":"Carol","gameId":"fortnite","region":"us-east-1"}' | pretty

step "Queue depth"
curl -s "$BASE_URL/api/v1/matchmaking/queue?gameId=fortnite&region=us-east-1" | pretty

step "Run the matcher, Carol gets a fresh session as host"
curl -s -X POST "$BASE_URL/api/v1/matchmaking/match?gameId=fortnite&region=us-east-1" | pretty

step "Bob leaves"
curl -s -X POST "$BASE_URL/api/v1/sessions/$CODE/leave?playerId=bob" | pretty

step "Alice terminates the session"
curl -s -X DELETE "$BASE_URL/api/v1/sessions/$CODE?playerId=alice" | pretty

step "Skill based matchmaking: four players of similar rating queue up"
for entry in "dana:1480" "eli:1495" "faye:1510" "gus:1470"; do
  name="${entry%%:*}"
  rating="${entry##*:}"
  curl -s -X POST "$BASE_URL/api/v1/matchmaking/skill/enqueue" \
    -H 'Content-Type: application/json' \
    -d "{\"playerId\":\"$name\",\"displayName\":\"${name}\",\"gameId\":\"apex-legends\",\"region\":\"us-west-2\",\"skillRating\":$rating}" | pretty
done

step "And one who is nowhere near them"
curl -s -X POST "$BASE_URL/api/v1/matchmaking/skill/enqueue" \
  -H 'Content-Type: application/json' \
  -d '{"playerId":"novice","displayName":"Novice","gameId":"apex-legends","region":"us-west-2","skillRating":600}' | pretty

step "Skill queue depth and the current search window"
curl -s "$BASE_URL/api/v1/matchmaking/skill/queue?gameId=apex-legends&region=us-west-2" | pretty

step "Form a party of four. The 600 rated player is left queued, not forced in"
curl -s -X POST "$BASE_URL/api/v1/matchmaking/skill/match?gameId=apex-legends&region=us-west-2&partySize=4" | pretty

step "One player still waiting; their window widens until somebody comparable arrives"
curl -s "$BASE_URL/api/v1/matchmaking/skill/queue?gameId=apex-legends&region=us-west-2" | pretty

step "Is the analytics tee reaching Kafka?"
curl -s "$BASE_URL/actuator/health" | pretty

step "Custom Prometheus metrics"
curl -s "$BASE_URL/actuator/prometheus" | grep -E '^cloudrelay_(sessions|players|matchmaking|analytics)' || true

printf '\nDone. Grafana dashboard at http://localhost:3000\n'
printf 'Every event above was teed to Kafka. To land it in Delta and query it:\n'
printf '  docker compose --profile lakehouse up -d --build\n'
printf '  ./scripts/lakehouse.sh query\n'
