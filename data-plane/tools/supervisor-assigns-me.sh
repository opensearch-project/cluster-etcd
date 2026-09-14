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
SUPERVISOR="${SUPERVISOR:-$ROOT/../opensearch-supervisor}"
OPENSEARCH="${OPENSEARCH:-$ROOT/../OpenSearch}"
STORE_DIR="${1:-/tmp/dp-assign}"
OUT="$ROOT/build/jbuild"
NODE_ID="dp-node-0"

JAVA="$(/usr/libexec/java_home -v 21 2>/dev/null || echo "")/bin/java"
[[ -x "$JAVA" ]] || JAVA=java

if [[ ! -d "$OUT/tools" ]]; then
    echo "Build first: tools/jbuild.sh --no-tests" >&2
    exit 1
fi
if [[ ! -d "$SUPERVISOR/build/jbuild/supervisor-app/main" ]]; then
    echo "Build the supervisor first: (cd $SUPERVISOR && tools/jbuild.sh --no-tests)" >&2
    exit 1
fi

jars() { find "$@" -name '*.jar' ! -name '*-sources.jar' ! -name '*-javadoc.jar' 2>/dev/null | tr '\n' ':'; }
cached() { find "$HOME/.gradle/caches/modules-2/files-2.1/$1/$2/$3" -name "$2-$3.jar" 2>/dev/null | head -1; }

PLUGIN_CP="$OUT/plugin/main:$OUT/tools:$OUT/store-api/main:$(jars \
    "$OPENSEARCH/server/build/distributions" "$OPENSEARCH/libs/core/build/distributions" \
    "$OPENSEARCH/libs/common/build/distributions" "$OPENSEARCH/libs/x-content/build/distributions")"
PLUGIN_CP="$PLUGIN_CP$(cached org.apache.logging.log4j log4j-api 2.25.4):"
PLUGIN_CP="$PLUGIN_CP$(cached org.apache.logging.log4j log4j-core 2.25.4):"
PLUGIN_CP="$PLUGIN_CP$(cached tools.jackson.core jackson-core 3.2.2):"
PLUGIN_CP="$PLUGIN_CP$(cached tools.jackson.dataformat jackson-dataformat-cbor 3.2.2):"
PLUGIN_CP="$PLUGIN_CP$(cached tools.jackson.dataformat jackson-dataformat-smile 3.2.2):"
PLUGIN_CP="$PLUGIN_CP$(cached tools.jackson.dataformat jackson-dataformat-yaml 3.2.2):"
PLUGIN_CP="$PLUGIN_CP$(find "$HOME/.gradle/caches/modules-2/files-2.1/org.apache.lucene" -name '*-10.5.1.jar' \
    ! -name '*-sources.jar' | tr '\n' ':')"

S="$SUPERVISOR/build/jbuild"
SUPERVISOR_CP="$S/supervisor-app/main:$S/supervisor-core/main:$S/supervisor-model/main:$S/store-api/main:$(find \
    "$HOME/.gradle/caches/modules-2/files-2.1/com.fasterxml.jackson.core" \
    \( -name 'jackson-databind-2.22.1.jar' -o -name 'jackson-core-2.22.1.jar' -o -name 'jackson-annotations-2.22.jar' \) \
    | tr '\n' ':')"

# The plugin logs through log4j, and a plain JVM has no configuration for it -- so without this the one thing
# this script exists to show would be swallowed.
LOG_CONFIG="$OUT/log4j2-tools.xml"
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
"$JAVA" -cp "$SUPERVISOR_CP" org.opensearch.supervisor.app.Main \
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
