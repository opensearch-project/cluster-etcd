#!/usr/bin/env bash
#
# bin/e2e.sh [--kafka]
#
# DEMO.md's steps against the cluster bin/up.sh brought up, then two failures:
#
#   1. create an index through the control plane, with a search replica per shard
#   2. see the supervisor place every copy, primaries and search replicas
#   3. write documents through the ingestion source (a file per shard, or with --kafka a Kafka partition per
#      shard, against a cluster brought up with bin/up.sh --kafka), count and search them through a coordinator,
#      and see each search replica reach the same count from the segment store
#   4. kill the data node holding shard 0's primary; it comes back under the same node id, keeps its copies,
#      and resumes ingesting
#   5. kill the supervisor holding the pool's lease; the other takes it over, and places a new index
#
# Exits non-zero at the first check that fails. Leaves nothing behind on success; KEEP=1 keeps the indices.
set -euo pipefail
source "$(dirname "$0")/common.sh"

source_type=file
for arg in "$@"; do
  case "$arg" in
    --kafka) source_type=kafka ;;
    *) die "unknown option: $arg" ;;
  esac
done

run="e2e-$(date +%s)"
index="$run"
shards=5
search_replicas=1
timeout="${E2E_TIMEOUT:-180}"

# The control plane has no published port: reach it through a port-forward that lives as long as this script.
forward_pid=""
trap '[ -z "$forward_pid" ] || kill "$forward_pid" 2>/dev/null || true' EXIT
start_control_plane_forward() {
  curl -sf -o /dev/null "$CONTROL_PLANE/" && die "something already answers at $CONTROL_PLANE (a forward left by an earlier run? see lsof -nP -iTCP:$CONTROL_PLANE_PORT -sTCP:LISTEN); stop it or set CONTROL_PLANE_PORT"
  # The kubectl binary itself, not the k() function: backgrounding a function forks a subshell, so $! would be the
  # subshell and killing it would leave kubectl running, holding the port.
  "${KUBECTL[@]}" -n "$NS" port-forward svc/control-plane "$CONTROL_PLANE_PORT:9250" >/dev/null 2>&1 &
  forward_pid=$!
  for _ in $(seq 1 30); do curl -sf -o /dev/null "$CONTROL_PLANE/" && return 0; sleep 1; done
  die "no control plane through a port-forward at $CONTROL_PLANE"
}

# --- helpers -----------------------------------------------------------------------------------------------

cp_api() { # method path [body]
  local args=(-sS -X "$1" -H 'Content-Type: application/json' "$CONTROL_PLANE$2")
  [ $# -ge 3 ] && args+=(-d "$3")
  curl "${args[@]}"
}
on_node() { # pod path: a request to one data node, from inside its pod, as the control plane's tenant
  k exec "$1" -c opensearch -- curl -sS -H 'x-tenant-id: default' "localhost:9200$2"
}
store_get() { # key, under the store's /opensearch root
  k exec etcd-0 -- etcdctl get --print-value-only "/opensearch/$1"
}
json() { # expression over stdin's JSON, as `d`
  python3 -c "import json, sys; d = json.load(sys.stdin); print($1)"
}
wait_for() { # description, then a command that succeeds once the condition holds
  local what="$1"; shift
  local deadline=$((SECONDS + timeout))
  until "$@"; do
    if [ "$SECONDS" -ge "$deadline" ]; then
      [ -z "${ON_TIMEOUT:-}" ] || $ON_TIMEOUT >&2 || true
      die "timed out after ${timeout}s waiting for: $what"
    fi
    sleep 2
  done
  echo "ok: $what" >&2
}
data_pods() { k get pods -l app=data-node -o jsonpath='{.items[*].metadata.name}'; }
doc() { # shard, n: one ingestion message
  printf '{"_id": "%s-%s", "_op_type": "index", "_source": {"title": "document %s of shard %s"}}\n' "$1" "$2" "$2" "$1"
}
# The file source's files live in the cluster (every data node mounts the same directory), so they are written
# through a data pod. data-0 is ready whenever this is called: the test waits for it after restarting it.
in_ingestion_dir() { k exec -i data-0 -c opensearch -- "$@"; }
# Writes documents first..last to every shard of the stream: the same number to each, which the checks rely on.
write_round() { # stream, first, last
  local s n
  case "$source_type" in
    file)
      for ((s = 0; s < shards; s++)); do
        for ((n = $2; n <= $3; n++)); do doc "$s" "$n"; done \
          | in_ingestion_dir sh -c "cat >> /clusterless-ingestion/$1/$s.ndjson"
      done ;;
    kafka)
      # Round-robin over the topic's partitions from a fresh producer, so a multiple of the partition count
      # lands the same number in each, which is what the file source gets by being addressed per shard.
      for ((n = $2; n <= $3; n++)); do
        for ((s = 0; s < shards; s++)); do doc "$s" "$n"; done
      done | k exec -i kafka-0 -- /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 \
        --topic "$1" --producer-property partitioner.class=org.apache.kafka.clients.producer.RoundRobinPartitioner ;;
  esac
}
prepare_stream() { # stream: what the source reads, empty, and with a slot for every shard
  case "$source_type" in
    file)
      in_ingestion_dir sh -c "mkdir -p /clusterless-ingestion/$1 && cd /clusterless-ingestion/$1 \
        && for s in \$(seq 0 $((shards - 1))); do : > \$s.ndjson; done" ;;
    kafka)
      k exec kafka-0 -- /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists \
        --topic "$1" --partitions "$shards" --replication-factor 1 >&2 ;;
  esac
}
drop_stream() { # stream
  case "$source_type" in
    file) in_ingestion_dir rm -rf "/clusterless-ingestion/$1" ;;
    kafka) k exec kafka-0 -- /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic "$1" >/dev/null 2>&1 || true ;;
  esac
}
ingestion_source() { # stream: the index's ingestion_source setting
  case "$source_type" in
    file) echo '{"type": "file", "param": {"stream": "'"$1"'", "base_directory": "/clusterless-ingestion"}}' ;;
    # earliest, because the default is latest, and an index that starts after its first documents were produced
    # would skip them.
    kafka) echo '{"type": "kafka", "pointer.init.reset": "earliest", "param": {"topic": "'"$1"'", "bootstrap_servers": "kafka:9092"}}' ;;
  esac
}
create_index() { # name, stream
  cp_api PUT "/$1" '{
    "settings": {
      "index.number_of_shards": "'"$shards"'",
      "index.number_of_replicas": "0",
      "index.number_of_search_replicas": "'"$search_replicas"'",
      "index.replication.type": "SEGMENT",
      "ingestion_source": '"$(ingestion_source "$2")"'
    },
    "mappings": {"properties": {"title": {"type": "text"}}}
  }' | tee /dev/stderr | json 'd.get("acknowledged") is True' | grep -qx True || die "could not create $1"
  echo >&2
}
all_copies_started() { # index
  local rows
  rows="$(cp_api GET "/_cat/shards/$1?h=state" | grep -c '^STARTED' || true)"
  [ "$rows" -eq $((shards * (1 + search_replicas))) ]
}
coordinated_count() { on_node data-0 "/$index/_count" | json 'd.get("count", -1)'; }
count_is() { [ "$(coordinated_count)" = "$1" ]; }
# Each copy's document count, from each node's own shard-level stats: one line per copy, "pod shard role count".
# Not _count, which a coordinating node routes over the whole index from wherever it is asked.
copy_counts() {
  local pod
  for pod in $(data_pods); do
    on_node "$pod" "/_nodes/_local/stats/indices/docs?level=shards" | POD="$pod" PREFIX="default.$index." python3 -c '
import json, os, sys
d = json.load(sys.stdin)
for node in d.get("nodes", {}).values():
    for name, copies in node.get("indices", {}).get("shards", {}).items():
        if not name.startswith(os.environ["PREFIX"]):
            continue
        for entry in copies:
            for shard, stats in entry.items():
                routing = stats.get("routing", {})
                role = "p" if routing.get("primary") else "s"
                print(os.environ["POD"], shard, role, stats.get("docs", {}).get("count", 0))
'
  done
}
copies_count() { copy_counts | awk '{ total += $4 } END { print total + 0 }'; }
copies_count_is() { [ "$(copies_count)" = "$1" ]; }
node_id() { on_node "$1" "/_nodes/_local?filter_path=nodes.*.name" | json 'next(iter(d["nodes"]))'; }
pod_replaced() { local uid; uid="$(k get pod "$1" -o jsonpath='{.metadata.uid}' 2>/dev/null)" && [ -n "$uid" ] && [ "$uid" != "$2" ]; }
pod_ready() { [ "$(k get pod "$1" -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}' 2>/dev/null)" = True ]; }
lease_holder() { store_get supervisor/pools/shared-a/lease | json 'd["holder"]'; }
lease_held_by_other_than() { local h; h="$(lease_holder 2>/dev/null)" && [ -n "$h" ] && [ "$h" != "$1" ]; }

# --- 0. the cluster is up ----------------------------------------------------------------------------------

log "0. preflight"
start_control_plane_forward
# 503 until a supervisor has published status/cluster, which is correct and looks like a bug.
wait_for "cluster health answers (a supervisor is publishing)" \
  sh -c "curl -sf -o /dev/null '$CONTROL_PLANE/_cluster/health'"
prepare_stream "$run"

# --- 1, 2. create an index; the supervisor places it --------------------------------------------------------

log "1. create $index through the control plane"
create_index "$index" "$run"

log "2. the supervisor places every copy"
wait_for "$((shards * (1 + search_replicas))) copies of $index started" all_copies_started "$index"
cp_api GET "/_cat/shards/$index?v&h=index,shard,prirep,state,node"

# --- 3. ingest, count, search -------------------------------------------------------------------------------

log "3. write 2 documents per shard through the $source_type source"
write_round "$run" 1 2
expected=$((shards * 2))
wait_for "a coordinator counts $expected documents" count_is "$expected"
hits="$(on_node data-1 "/$index/_search?q=title:document&size=0" | json 'd["hits"]["total"]["value"]')"
[ "$hits" = "$expected" ] || die "search through data-1 found $hits documents, expected $expected"
echo "ok: a search through another coordinator finds $hits" >&2
show_copies() { echo "pod shard role docs"; copy_counts | sort -k2,2n -k3,3; }
ON_TIMEOUT=show_copies wait_for "every search replica has its primary's documents, from the segment store" \
  copies_count_is $((expected * (1 + search_replicas)))
show_copies >&2

# --- 4. kill a data node ------------------------------------------------------------------------------------

log "4. kill the data node holding shard 0's primary"
victim_node="$(cp_api GET "/_cat/shards/$index?h=shard,prirep,node" | awk '$1 == 0 && $2 == "p" { print $3 }')"
[ -n "$victim_node" ] || die "no primary for shard 0 in _cat/shards"
victim=""
for pod in $(data_pods); do
  if [ "$(node_id "$pod")" = "$victim_node" ] || [ "$pod" = "$victim_node" ]; then victim="$pod"; fi
done
[ -n "$victim" ] || die "no pod is node $victim_node"
id_before="$(node_id "$victim")"
echo "shard 0's primary is on $victim, node id $id_before" >&2
old_uid="$(k get pod "$victim" -o jsonpath='{.metadata.uid}')"
k delete pod "$victim" --grace-period=1 --wait=false
wait_for "$victim is replaced" pod_replaced "$victim" "$old_uid"
wait_for "$victim is ready again" pod_ready "$victim"
id_after="$(node_id "$victim")"
[ "$id_after" = "$id_before" ] || die "$victim came back as node $id_after, was $id_before: its disk was lost"
echo "ok: $victim kept its node id across the restart" >&2
wait_for "every copy is started again" all_copies_started "$index"
write_round "$run" 3 3
expected=$((expected + shards))
wait_for "ingestion resumes: a coordinator counts $expected documents" count_is "$expected"

# --- 5. kill the supervisor holding the lease ---------------------------------------------------------------

log "5. kill the supervisor holding pool shared-a"
holder="$(lease_holder)"
holder_pod="${holder%-*}"
k get pod "$holder_pod" >/dev/null 2>&1 || die "lease holder $holder is not a pod of this cluster"
echo "pool shared-a is held by $holder (pod $holder_pod)" >&2
k delete pod "$holder_pod" --grace-period=0 --force --wait=false 2>/dev/null
wait_for "another supervisor holds pool shared-a" lease_held_by_other_than "$holder"
echo "pool shared-a is now held by $(lease_holder)" >&2
second="$run-after-failover"
prepare_stream "$second"
create_index "$second" "$second"
wait_for "the new holder places $second" all_copies_started "$second"

# --- done ---------------------------------------------------------------------------------------------------

if [ -z "${KEEP:-}" ]; then
  cp_api DELETE "/$index" >/dev/null
  cp_api DELETE "/$second" >/dev/null
  drop_stream "$run"
  drop_stream "$second"
fi
log "passed"
