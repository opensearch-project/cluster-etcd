#!/usr/bin/env bash
#
# Puts the S3 client's credentials in the keystore, which is the only place repository-s3 reads them from,
# and starts the node. Arguments are passed to bin/opensearch, so `-Esetting=value` works.
set -euo pipefail
cd /usr/share/opensearch

if [ -n "${S3_ACCESS_KEY:-}" ] || [ -n "${S3_SECRET_KEY:-}" ]; then
  : "${S3_ACCESS_KEY:?S3_SECRET_KEY is set, so S3_ACCESS_KEY must be too}"
  : "${S3_SECRET_KEY:?S3_ACCESS_KEY is set, so S3_SECRET_KEY must be too}"
  # Rebuilt on every start, from the environment, so a changed secret takes effect on restart.
  rm -f config/opensearch.keystore
  bin/opensearch-keystore create
  printf '%s' "$S3_ACCESS_KEY" | bin/opensearch-keystore add --stdin s3.client.default.access_key
  printf '%s' "$S3_SECRET_KEY" | bin/opensearch-keystore add --stdin s3.client.default.secret_key
fi

exec bin/opensearch "$@"
