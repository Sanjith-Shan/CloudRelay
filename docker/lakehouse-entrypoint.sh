#!/usr/bin/env bash
# Runs the medallion pipeline. Waits for Kafka first, because a Spark job that
# starts before the broker is reachable fails on its first batch and takes
# thirty seconds of driver startup with it.
set -euo pipefail

KAFKA="${CLOUDRELAY_KAFKA_BOOTSTRAP:-kafka:29092}"
BASE_PATH="${CLOUDRELAY_LAKEHOUSE_PATH:-/data/lakehouse}"
COMMAND="${1:-run}"
shift || true

host="${KAFKA%%:*}"
port="${KAFKA##*:}"
echo "[lakehouse] waiting for kafka at ${host}:${port}"
for _ in $(seq 1 60); do
  if (echo > "/dev/tcp/${host}/${port}") >/dev/null 2>&1; then
    echo "[lakehouse] kafka is up"
    break
  fi
  sleep 2
done

# Spark on Java 17 needs the module system opened up. The same list lives in
# the lakehouse pom's surefire argLine; if one changes, change both.
exec java \
  --add-opens=java.base/java.lang=ALL-UNNAMED \
  --add-opens=java.base/java.lang.invoke=ALL-UNNAMED \
  --add-opens=java.base/java.lang.reflect=ALL-UNNAMED \
  --add-opens=java.base/java.io=ALL-UNNAMED \
  --add-opens=java.base/java.net=ALL-UNNAMED \
  --add-opens=java.base/java.nio=ALL-UNNAMED \
  --add-opens=java.base/java.util=ALL-UNNAMED \
  --add-opens=java.base/java.util.concurrent=ALL-UNNAMED \
  --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED \
  --add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
  --add-opens=java.base/sun.nio.cs=ALL-UNNAMED \
  --add-opens=java.base/sun.security.action=ALL-UNNAMED \
  --add-opens=java.base/sun.util.calendar=ALL-UNNAMED \
  -Djdk.reflect.useDirectMethodHandle=false \
  -cp "/app/lakehouse.jar:/app/libs/*" \
  com.cloudrelay.lakehouse.MedallionPipeline \
  "$COMMAND" \
  --base-path "$BASE_PATH" \
  --kafka "$KAFKA" \
  "$@"
