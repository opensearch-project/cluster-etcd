# Sourced by the scripts in this directory.
#
# Two ways to get a local Kubernetes cluster, chosen by CLUSTER_RUNTIME:
#   kind       the default: kind, on whatever Docker daemon `docker` reaches (Colima, Docker Desktop, ...)
#   container  Apple's `container cluster` (EXPERIMENTAL), which needs no Docker daemon
# Everything above this layer -- the manifests, up.sh's steps, the end-to-end test -- is the same for both.
# A cd that finds its directory through CDPATH prints it, so `$(cd x && pwd)` would capture two lines. Every script
# sources this before its first cd.
unset CDPATH
DEPLOY="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPO="$(dirname "$DEPLOY")"
CLUSTER="${CLUSTER:-clusterless}"
CLUSTER_RUNTIME="${CLUSTER_RUNTIME:-kind}"
CLUSTER_CPUS="${CLUSTER_CPUS:-8}"
CLUSTER_MEMORY="${CLUSTER_MEMORY:-16G}"
NS=clusterless
CONTROL_PLANE_PORT="${CONTROL_PLANE_PORT:-9250}"
CONTROL_PLANE="${CONTROL_PLANE:-http://localhost:$CONTROL_PLANE_PORT}"
TAG=dev
IMAGES=(control-plane supervisor data-node)

log() { printf '\n==> %s\n' "$*" >&2; }
die() { printf 'error: %s\n' "$*" >&2; exit 1; }

case "$CLUSTER_RUNTIME" in
  kind)
    KUBECTL=(kubectl --context "kind-$CLUSTER")
    runtime_tools=(docker kind kubectl) ;;
  container)
    # `container cluster write-config` writes the context to a kubeconfig of our choosing, which is used for every
    # kubectl here. (`cluster create` also adds the context to ~/.kube/config.)
    KUBECONFIG_FILE="$DEPLOY/build/kubeconfig-$CLUSTER"
    # The file has the cluster's context but no current-context, so it is named here; without that kubectl
    # ignores the file and tries localhost:8080.
    KUBECTL=(kubectl --kubeconfig "$KUBECONFIG_FILE" --context "$CLUSTER")
    runtime_tools=(container kubectl) ;;
  *) die "CLUSTER_RUNTIME must be kind or container, not $CLUSTER_RUNTIME" ;;
esac

kc() { "${KUBECTL[@]}" "$@"; }
k() { kc -n "$NS" "$@"; }

cluster_exists() {
  case "$CLUSTER_RUNTIME" in
    kind) kind get clusters 2>/dev/null | grep -qx "$CLUSTER" ;;
    container) container cluster list 2>/dev/null | awk 'NR > 1 { print $1 }' | grep -qx "$CLUSTER" ;;
  esac
}
cluster_create() {
  case "$CLUSTER_RUNTIME" in
    kind) kind create cluster --name "$CLUSTER" --wait 120s ;;
    container)
      container cluster create --name "$CLUSTER" --cpus "$CLUSTER_CPUS" --memory "$CLUSTER_MEMORY"
      mkdir -p "$DEPLOY/build"
      container cluster write-config --name "$CLUSTER" --kubeconfig "$KUBECONFIG_FILE"
      kc wait --for=condition=Ready node --all --timeout=180s ;;
  esac
}
# A default StorageClass, which every volume claim here needs. kind applies the local-path provisioner that its
# node image carries; `container cluster` uses the same node image and does not, so it is applied from the node.
# Idempotent, so it also repairs a cluster created before this existed.
cluster_ensure_storage() {
  case "$CLUSTER_RUNTIME" in
    kind) ;;
    container)
      kc get storageclass standard >/dev/null 2>&1 && return 0
      container exec "$CLUSTER" cat /kind/manifests/default-storage.yaml | kc apply -f - \
        || die "could not apply the node image's /kind/manifests/default-storage.yaml; no volume claim will bind" ;;
  esac
}
cluster_delete() {
  case "$CLUSTER_RUNTIME" in
    kind) kind delete cluster --name "$CLUSTER" ;;
    container) container cluster delete --name "$CLUSTER"; rm -f "$KUBECONFIG_FILE" ;;
  esac
}
# The reference an image is stored under in the cluster's containerd, which is what a pod's `image:` names after
# Kubernetes qualifies it. Docker and kind keep an unqualified name as it is. `container` adds its own default
# registry to one, which no pod would ask for, so it is qualified here.
image_ref() {
  case "$CLUSTER_RUNTIME" in
    kind) echo "$1" ;;
    container) echo "docker.io/$1" ;;
  esac
}
# Whether an image is in the local store of whatever builds the images, by reference.
image_exists() {
  case "$CLUSTER_RUNTIME" in
    kind) docker image inspect "$1" >/dev/null 2>&1 ;;
    container) container image inspect "$(image_ref "$1")" >/dev/null 2>&1 ;;
  esac
}
image_build() { # reference, context directory
  case "$CLUSTER_RUNTIME" in
    kind) docker build -t "$1" "$2" ;;
    container) container build -t "$(image_ref "$1")" "$2" ;;
  esac
}
cluster_load_image() {
  case "$CLUSTER_RUNTIME" in
    kind) kind load docker-image --name "$CLUSTER" "$1" ;;
    # What `kind load` does: export the image and import it into the node's containerd, in the namespace the
    # kubelet uses. Not --shared-image-store, which in 1.7.1.2 still sent the kubelet to the registry for an image
    # built on the host. One node: the node container is named after the cluster.
    container)
      local tar
      tar="$(mktemp "${TMPDIR:-/tmp}/clusterless-image.XXXXXX")" || die "could not create a temporary file"
      # Tested by `if`, where errexit does not apply: as a bare `&&` list, a failing import would end the script
      # here, before the tarball (the size of the image) is removed or the reason printed.
      if ! { container image save -o "$tar" "$(image_ref "$1")" >/dev/null \
          && container exec -i "$CLUSTER" ctr -n k8s.io images import --all-platforms - < "$tar" >/dev/null; }; then
        rm -f "$tar"
        die "could not load $(image_ref "$1") into the cluster node"
      fi
      rm -f "$tar" ;;
  esac
}
