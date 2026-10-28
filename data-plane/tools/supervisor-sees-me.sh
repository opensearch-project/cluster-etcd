#!/usr/bin/env bash
#
# Milestone 2's acceptance check: does the real supervisor see this node?
#
# Publishes node documents into a scratch store with the plugin's own publisher, seeds the pool and tenant
# an operator would have created, runs a real supervisor against it in its default read-only mode, and
# prints the convergence projection. A pass looks like:
#
#     "nodes": 2, "placeable_nodes": 2
#
# with UNASSIGNED_PRIMARY findings for the index that has nowhere to live yet -- which is the supervisor
# saying it has somewhere to put shards and is waiting to be allowed to.
#
# No OpenSearch node and no container: the only part of the plugin involved is the part that talks to the
# store, which is exactly the part this milestone is about.
#
# This seeds its own pool definition, because a store with no pool has no pools and therefore no reconciliation.
# For a real deployment the command is the supervisor's:
#
#     supervisor-app --store=<uri> --define-pool=shared-a
#
# Usage:
#   tools/supervisor-sees-me.sh [store-directory] [node-count]
#
set -euo pipefail

cd "$(dirname "$0")/.."
ROOT="$PWD"
REPO="$(cd .. && pwd)"
STORE_DIR="${1:-/tmp/dp-demo}"
NODES="${2:-2}"
JAVA_HOME="$(/usr/libexec/java_home -v 21 2>/dev/null || echo "${JAVA_HOME:-}")"
export JAVA_HOME
JAVA="${JAVA_HOME:+$JAVA_HOME/bin/}java"

# One build, from the repository root: the plugin's tools and their classpath, and the supervisor's launcher.
echo "==> building the plugin's tools and the supervisor"
(cd "$REPO" && ./gradlew -q :data-plane:plugin:toolsClasspath :supervisor:supervisor-app:installDist)
PLUGIN_CP="$(cat "$ROOT/plugin/build/tools/classpath.txt")"
SUPERVISOR_APP="$REPO/supervisor/supervisor-app/build/install/supervisor-app/bin/supervisor-app"

echo "==> a clean store at $STORE_DIR"
rm -rf "$STORE_DIR"
mkdir -p "$STORE_DIR"

echo "==> publishing $NODES node(s) with the plugin's publisher"
"$JAVA" -cp "$PLUGIN_CP" tools.PublishNodes "file://$STORE_DIR" "$NODES"

echo
echo "==> running a real supervisor, read-only"
"$SUPERVISOR_APP" \
    --store="file://$STORE_DIR" --max-passes=3 --interval=200ms

echo
echo "==> supervisor/status/pools/shared-a/convergence"
# The filesystem backend percent-encodes the key into a filename and prefixes the value with a revision
# header, so the JSON starts at the first brace rather than at byte zero.
python3 - "$STORE_DIR" <<'PY'
import json, sys, pathlib
path = pathlib.Path(sys.argv[1]) / "supervisor%2Fstatus%2Fpools%2Fshared-a%2Fconvergence"
if not path.exists():
    print("no convergence projection was written; the supervisor never owned the pool", file=sys.stderr)
    raise SystemExit(1)
raw = path.read_text()
document = json.loads(raw[raw.index("{"):])
for field in ("nodes", "placeable_nodes", "indices", "findings_by_kind", "unreadable_documents"):
    print(f"  {field}: {json.dumps(document.get(field))}")
if document.get("placeable_nodes", 0) < 1:
    print("\nFAILED: the supervisor sees no placeable node", file=sys.stderr)
    raise SystemExit(1)
print("\nOK: the supervisor sees this node as live and placeable.")
PY
