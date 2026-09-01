#!/usr/bin/env bash
#
# Copyright OpenSearch Software Foundation
# SPDX-License-Identifier: Apache-2.0
#
# Runs the opensearch-api-specification test harness against this control plane.
#
# The harness is the same one the spec repo uses to validate OpenSearch itself: it replays YAML
# story files over HTTP and validates both status codes and response bodies against the spec.
# Running it against this service is what proves the two surfaces agree, rather than us asserting
# they do.
#
# Usage:
#   conformance/run.sh [--url URL] [--spec-repo PATH] [--all] [-- <extra tester args>]
#
#   --url URL          control plane base URL (default http://localhost:9200)
#   --spec-repo PATH   opensearch-api-specification checkout (default ../opensearch-api-specification)
#   --all              run every story in the spec repo in one pass, not just the manifest.
#                      Expect large numbers of failures: everything unimplemented answers 501.
#                      Useful as a to-do list, not as a pass/fail gate.
#
# Anything after -- is passed through to the tester, e.g. --verbose or --coverage-report.

set -euo pipefail

URL="http://localhost:9200"
SPEC_REPO=""
RUN_ALL=0
EXTRA_ARGS=()

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "$script_dir/.." && pwd)"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --url) URL="$2"; shift 2 ;;
    --spec-repo) SPEC_REPO="$2"; shift 2 ;;
    --all) RUN_ALL=1; shift ;;
    --) shift; EXTRA_ARGS=("$@"); break ;;
    -h|--help) sed -n '7,23p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) echo "unknown option: $1" >&2; exit 2 ;;
  esac
done

if [[ -z "$SPEC_REPO" ]]; then
  SPEC_REPO="$(cd "$repo_root/.." && pwd)/opensearch-api-specification"
fi

# --- preflight -------------------------------------------------------------

if [[ ! -d "$SPEC_REPO/spec" ]]; then
  cat >&2 <<EOF
error: no spec repo at $SPEC_REPO

The harness lives in the opensearch-api-specification repo. Clone it and point at it:

  git clone https://github.com/opensearch-project/opensearch-api-specification
  conformance/run.sh --spec-repo /path/to/opensearch-api-specification
EOF
  exit 1
fi

if [[ ! -d "$SPEC_REPO/node_modules" ]]; then
  echo "error: $SPEC_REPO has no node_modules; run 'npm install' there first" >&2
  exit 1
fi

if ! curl -fsS -m 5 -o /dev/null "$URL" 2>/dev/null; then
  cat >&2 <<EOF
error: no control plane responding at $URL

Start it first:

  ./gradlew :server:run --args="--port 9200 --data-path /tmp/cp-conformance"

A throwaway --data-path is worth using: the harness creates and deletes real resources.
EOF
  exit 1
fi

run_tester() {
  # ${arr[@]+...} guards against set -u treating an empty array as unbound, which bash 3.2 (the
  # macOS default) does.
  (cd "$SPEC_REPO" && npx ts-node tools/src/tester/test.ts \
    --spec "$SPEC_REPO/spec" \
    --tests-path "$1" \
    --opensearch-url "$URL" \
    ${EXTRA_ARGS[@]+"${EXTRA_ARGS[@]}"})
}

# --- run -------------------------------------------------------------------

if [[ "$RUN_ALL" == "1" ]]; then
  echo "Running ALL stories against $URL (expect 501 failures for unimplemented operations)"
  run_tester "$SPEC_REPO/tests"
  exit $?
fi

# Stories are run in place, one tester invocation each, rather than staged into a temp tree. The
# tester validates each story's $schema field against the file's own location, so a copied story
# fails validation before it ever runs. One invocation per story costs a spec parse each (a few
# seconds); use --all for a single pass over everything.
manifest="$script_dir/stories.txt"
stories=()
while IFS= read -r line || [[ -n "$line" ]]; do
  line="${line%%#*}"
  line="$(echo "$line" | xargs || true)"
  [[ -z "$line" ]] && continue
  src="$SPEC_REPO/tests/$line"
  if [[ ! -f "$src" ]]; then
    echo "error: story not found: $src" >&2
    echo "       (listed in $manifest -- has the spec repo renamed or moved it?)" >&2
    exit 1
  fi
  stories+=("$line")
done < "$manifest"

if [[ "${#stories[@]}" == "0" ]]; then
  echo "error: no stories listed in $manifest" >&2
  exit 1
fi

echo "Running ${#stories[@]} story file(s) from $manifest against $URL"
echo

failed=()
for story in "${stories[@]}"; do
  echo "=== $story"
  if run_tester "$SPEC_REPO/tests/$story"; then
    :
  else
    failed+=("$story")
  fi
  echo
done

echo "──────────────────────────────────────────────"
if [[ "${#failed[@]}" == "0" ]]; then
  echo "conformance: ${#stories[@]}/${#stories[@]} stories passed"
  exit 0
fi
echo "conformance: $((${#stories[@]} - ${#failed[@]}))/${#stories[@]} stories passed, ${#failed[@]} failed:"
for story in "${failed[@]}"; do
  echo "  - $story"
done
exit 1
