#!/usr/bin/env bash
#
# Milestone 3's acceptance check: does a real supervisor assign a shard, and does this node explain what it
# would do with it?
#
#   1. publishes node documents with the plugin's own publisher, and seeds a pool, a tenant and an index
#   2. runs a real supervisor in --write mode until it writes an assignment
#   3. runs one read-only pass for the node, which prints the cluster state it would have applied
#
# The interesting part is step 3's output. It should describe the index the supervisor assigned -- settings,
# mappings, roles, primary terms, recovery sources -- and say plainly that nothing was applied.
#
# No OpenSearch node and no container. Everything from the store to the fabricated ClusterState is ordinary
# Java; only handing that state to the applier needs a node, and that is the next milestone.
#
# Usage:
#   tools/supervisor-assigns-me.sh [store-directory]
#
set -euo pipefail

cd "$(dirname "$0")/.."
ROOT="$PWD"
REPO="$(cd .. && pwd)"
STORE_DIR="${1:-/tmp/dp-assign}"
NODE_ID="dp-node-0"
JAVA_HOME="$(/usr/libexec/java_home -v 21 2>/dev/null || echo "${JAVA_HOME:-}")"
export JAVA_HOME
JAVA="${JAVA_HOME:+$JAVA_HOME/bin/}java"

# One build, from the repository root: the plugin's tools and their classpath, and the supervisor's launcher.
echo "==> building the plugin's tools and the supervisor"
(cd "$REPO" && ./gradlew -q :data-plane:plugin:toolsClasspath :supervisor:supervisor-app:installDist)
PLUGIN_CP="$(cat "$ROOT/plugin/build/tools/classpath.txt")"
SUPERVISOR_APP="$REPO/supervisor/supervisor-app/build/install/supervisor-app/bin/supervisor-app"

# The plugin logs through log4j, and a plain JVM has no configuration for it -- so without this the one thing
# this script exists to show would be swallowed.
LOG_CONFIG="$ROOT/plugin/build/tools/log4j2-tools.xml"
cat > "$LOG_CONFIG" <<'XML'
<?xml version="1.0" encoding="UTF-8"?>
<Configuration status="ERROR">
  <Appenders>
    <Console name="console" target="SYSTEM_OUT">
      <PatternLayout pattern="%m%n"/>
    </Console>
  </Appenders>
  <Loggers>
    <Root level="info"><AppenderRef ref="console"/></Root>
  </Loggers>
</Configuration>
XML

echo "==> a clean store at $STORE_DIR"
rm -rf "$STORE_DIR"
mkdir -p "$STORE_DIR"

echo "==> publishing one node, and seeding a pool, a tenant and an index"
"$JAVA" -cp "$PLUGIN_CP" -Dlog4j2.configurationFile="file:$LOG_CONFIG" tools.PublishNodes "file://$STORE_DIR" 1

echo
echo "==> running a real supervisor in --write mode, so it assigns the shards"
"$SUPERVISOR_APP" \
    --store="file://$STORE_DIR" --write --max-passes=5 --interval=200ms 2>&1 | grep -E "assign|plan|wrote|level=warn|level=error" || true

echo
echo "==> what the supervisor wrote for $NODE_ID"
python3 - "$STORE_DIR" "$NODE_ID" <<'PY'
import json, sys, pathlib
path = pathlib.Path(sys.argv[1]) / f"nodes%2Fassignment%2F{sys.argv[2]}"
if not path.exists():
    print("NO ASSIGNMENT was written; the supervisor did not consider this node placeable", file=sys.stderr)
    raise SystemExit(1)
raw = path.read_text()
print(json.dumps(json.loads(raw[raw.index("{"):]), indent=2))
PY

echo
echo "==> one read-only pass for $NODE_ID"
"$JAVA" -cp "$PLUGIN_CP" -Dlog4j2.configurationFile="file:$LOG_CONFIG" tools.PollAssignment "file://$STORE_DIR" "$NODE_ID"
