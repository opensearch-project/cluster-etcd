#!/usr/bin/env bash
# Deletes the cluster, and with it every volume: the store, the segments and each node's data.
set -euo pipefail
source "$(dirname "$0")/common.sh"
cluster_delete
