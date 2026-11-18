#!/usr/bin/env bash
#
# bin/up.sh [--restart] [--kafka]
#
# Brings the whole system up on a local Kubernetes cluster (CLUSTER_RUNTIME=kind or container), from images bin/build-images.sh built: etcd, five data nodes,
# two supervisors (and the pool they serve), and two control planes, whose capabilities file is generated
# from the running data nodes. --kafka adds a Kafka broker. Safe to re-run. --restart also restarts every workload running one of our
# images, which is how a rebuilt image (same :dev tag) reaches running pods.
set -euo pipefail
source "$(dirname "$0")/common.sh"

restart=false
overlay=base
for arg in "$@"; do
  case "$arg" in
    --restart) restart=true ;;
    --kafka) overlay=kafka ;;
    *) die "unknown option: $arg" ;;
  esac
done

for tool in "${runtime_tools[@]}" curl python3; do
  command -v "$tool" >/dev/null || die "$tool is not installed"
done
for image in "${IMAGES[@]}"; do
  image_exists "clusterless/$image:$TAG" || die "no image clusterless/$image:$TAG; run bin/build-images.sh first"
done

mkdir -p "$DEPLOY/build"
if ! cluster_exists; then
  log "creating $CLUSTER_RUNTIME cluster $CLUSTER"
  cluster_create
fi

cluster_ensure_storage

log "loading images into the cluster"
for image in "${IMAGES[@]}"; do
  cluster_load_image "clusterless/$image:$TAG"
done

log "applying manifests"
# A Job's pod template cannot be changed in place, and both Jobs are safe to run again.
k delete job define-pool --ignore-not-found >/dev/null 2>&1 || true
kc apply -k "$DEPLOY/k8s/$overlay"
if $restart; then
  # Not the control plane: it reads its capabilities once, at startup, so it is restarted below, after they are
  # regenerated from the data nodes this restarts.
  k rollout restart statefulset/data deployment/supervisor
fi

log "waiting for etcd and the pool definition"
k rollout status statefulset/etcd --timeout=300s
k wait --for=condition=complete job/define-pool --timeout=300s

if [ "$overlay" = kafka ]; then
  log "waiting for Kafka"
  k rollout status statefulset/kafka --timeout=300s
fi

log "waiting for the data nodes"
k rollout status statefulset/data --timeout=600s
k rollout status deployment/supervisor --timeout=300s

# The control plane's capabilities, from every data node: the minimum node version among them is what new
# indices are created at, so it must come from what is running, not from an image.
log "generating the control plane's capabilities from the running data nodes"
generated="$DEPLOY/build/control-plane"
mkdir -p "$generated"
replicas="$(k get statefulset data -o jsonpath='{.spec.replicas}')"
pids=()
urls=()
# Waited for with stderr discarded, or bash reports each one it killed as "Terminated".
cleanup() {
  [ "${#pids[@]}" -eq 0 ] && return
  kill "${pids[@]}" 2>/dev/null || true
  wait "${pids[@]}" 2>/dev/null || true
}
trap cleanup EXIT
# One port-forward per node, each logged, and started again once if it does not answer: a forward can fail on its
# own (a port still held, a dropped stream) while the node is fine, and the log is the only place that says which.
forward() { # index
  local port=$((19200 + $1)) log="$generated/port-forward-data-$1.log" attempt
  for attempt in 1 2; do
    # kubectl itself, so that $! is its PID and the kill below ends it (a backgrounded function is a subshell).
    "${KUBECTL[@]}" -n "$NS" port-forward "pod/data-$1" "$port:9200" > "$log" 2>&1 &
    local pid=$!
    for _ in $(seq 1 20); do
      if curl -sf -o /dev/null "http://127.0.0.1:$port/"; then
        pids+=("$pid")
        urls+=("http://127.0.0.1:$port")
        return 0
      fi
      kill -0 "$pid" 2>/dev/null || break
      sleep 1
    done
    kill "$pid" 2>/dev/null || true
    wait "$pid" 2>/dev/null || true
    echo "port-forward to data-$1 on $port did not answer (attempt $attempt):" >&2
    sed 's/^/  /' "$log" >&2
  done
  die "could not reach data-$1 through a port-forward on 127.0.0.1:$port"
}
for ((i = 0; i < replicas; i++)); do forward "$i"; done
"$REPO/control-plane/tools/dump-capabilities.sh" "${urls[@]}" > "$generated/capabilities.yaml"
# Depends on a proposed upstream change. Without it the control plane runs permissive about settings, which is
# what an empty registry means to it.
if ! curl -sf "${urls[0]}/_cluster/settings_registry" -o "$generated/settings_registry.json"; then
  echo "this OpenSearch has no _cluster/settings_registry; the control plane will accept any setting" >&2
  echo '{}' > "$generated/settings_registry.json"
fi
cleanup
pids=()

before="$(k get configmap control-plane-capabilities -o jsonpath='{.metadata.resourceVersion}' 2>/dev/null || true)"
k create configmap control-plane-capabilities \
  --from-file="$generated/capabilities.yaml" --from-file="$generated/settings_registry.json" \
  --dry-run=client -o yaml | k apply -f -
after="$(k get configmap control-plane-capabilities -o jsonpath='{.metadata.resourceVersion}')"
# The control plane reads the file once, at startup.
if $restart || { [ -n "$before" ] && [ "$before" != "$after" ]; }; then
  k rollout restart deployment/control-plane
fi

log "waiting for the control plane"
k rollout status deployment/control-plane --timeout=300s
# From inside the cluster, so the check does not depend on a port-forward or on the host's view of the network.
for _ in $(seq 1 30); do
  k exec data-0 -c opensearch -- curl -sf -o /dev/null http://control-plane:9250/ && break
  sleep 1
done
k exec data-0 -c opensearch -- curl -sf -o /dev/null http://control-plane:9250/ \
  || die "control plane not answering inside the cluster"

log "up"
k get pods -o wide
cat <<EOF

Control plane: ${KUBECTL[*]} -n $NS port-forward svc/control-plane $CONTROL_PLANE_PORT:9250
               then $CONTROL_PLANE (tenant "default"; no authentication, so keep it on loopback)
File ingestion: <stream>/<shard>.ndjson under /clusterless-ingestion, in every data node
A data node:   ${KUBECTL[*]} -n $NS exec data-0 -- curl -s localhost:9200/_clusterless/status
End to end:    $DEPLOY/bin/e2e.sh
EOF
