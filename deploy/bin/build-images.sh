#!/usr/bin/env bash
#
# bin/build-images.sh /path/to/OpenSearch
#
# Stages and builds the three images: clusterless/control-plane, clusterless/supervisor, clusterless/data-node,
# all tagged :dev. The OpenSearch checkout must be at the version the plugin builds against (3.10.0-SNAPSHOT
# unless -Dopensearch.version says otherwise), published to mavenLocal, and have built:
#
#   ./gradlew publishToMavenLocal :distribution:archives:linux-arm64-tar:assemble \
#       :plugins:repository-s3:assemble :plugins:ingestion-kafka:assemble
#
# (linux-tar rather than linux-arm64-tar on an x86 machine).
set -euo pipefail
source "$(dirname "$0")/common.sh"

checkout="${1:-${OPENSEARCH_CHECKOUT:-}}"
[ -n "$checkout" ] || die "usage: $0 /path/to/OpenSearch (or set OPENSEARCH_CHECKOUT)"
# Absolute, because Gradle runs with -p deploy and would resolve a relative path against neither this shell's
# directory nor one place consistently.
# (A second variable: a failed assignment would empty $checkout before the message reads it.)
checkout_dir="$(cd -- "$checkout" && pwd)" || die "no such directory: $checkout"
checkout="$checkout_dir"

log "staging build contexts"
"$REPO/gradlew" -p "$DEPLOY" stage -Popensearch.checkout="$checkout"

for image in "${IMAGES[@]}"; do
  log "building clusterless/$image:$TAG"
  image_build "clusterless/$image:$TAG" "$DEPLOY/build/images/$image"
done
