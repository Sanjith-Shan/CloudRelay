#!/usr/bin/env bash
# Runs the medallion pipeline locally, without Docker.
#
#   ./scripts/lakehouse.sh run                       Kafka -> bronze -> silver -> gold
#   ./scripts/lakehouse.sh run --source file         replay from data/lakehouse/landing
#   ./scripts/lakehouse.sh query                     the analytics pack over gold
#   ./scripts/lakehouse.sh compact                   OPTIMIZE, with before/after numbers
#   ./scripts/lakehouse.sh history                   Delta version history
#
# Everything after the command is passed straight through, so --base-path,
# --kafka, --topic and --continuous all work.
set -euo pipefail

cd "$(dirname "$0")/.."

COMMAND="${1:-run}"
shift || true

BASE_PATH="${CLOUDRELAY_LAKEHOUSE_PATH:-data/lakehouse}"
KAFKA="${CLOUDRELAY_KAFKA_BOOTSTRAP:-localhost:9092}"
# Relative to the module, not the repo root: dependency:build-classpath
# resolves outputFile against the project directory it runs in.
CP_MODULE_PATH="target/classpath.txt"
CP_FILE="cloudrelay-lakehouse/$CP_MODULE_PATH"

# Spark 3.5 does not run on anything newer than Java 17, and Maven on this
# machine may well default to something newer. Pin it rather than fail later
# with a message that blames Scala.
if [ -z "${JAVA_HOME:-}" ] && [ -x /usr/libexec/java_home ]; then
  JAVA_HOME="$(/usr/libexec/java_home -v 17 2>/dev/null || true)"
  export JAVA_HOME
fi

# Always compile. Maven is quick when there is nothing to do, and the
# alternative — rebuilding only when the pom changes — silently runs stale
# classes after every source edit, which is a genuinely horrible way to spend
# an afternoon.
echo "[lakehouse] building"
mvn -q -B -pl cloudrelay-lakehouse -am -DskipTests package

# The dependency list only changes with the pom, and resolving it is the slow
# part, so that one is cached.
if [ ! -f "$CP_FILE" ] || [ cloudrelay-lakehouse/pom.xml -nt "$CP_FILE" ]; then
  echo "[lakehouse] resolving classpath"
  # includeScope=test because Spark itself is a `provided` dependency: it is
  # kept out of the packaged artifact on purpose, and a local run has no
  # cluster to get it from.
  mvn -q -B -pl cloudrelay-lakehouse dependency:build-classpath \
      -Dmdep.includeScope=test -Dmdep.outputFile="$CP_MODULE_PATH"
fi

CLASSPATH="cloudrelay-lakehouse/target/classes:$(cat "$CP_FILE")"

# Spark on Java 17 needs the module system opened up. The same list lives in
# the lakehouse pom's surefire argLine and in docker/lakehouse-entrypoint.sh.
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
  -cp "$CLASSPATH" \
  com.cloudrelay.lakehouse.MedallionPipeline \
  "$COMMAND" --base-path "$BASE_PATH" --kafka "$KAFKA" "$@"
