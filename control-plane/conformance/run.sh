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
#   conformance/run.sh [--url URL] [--reference URL] [--spec-repo PATH] [--all]
#                      [-- <extra tester args>]
#
#   --url URL          control plane base URL (default http://localhost:9400)
#   --reference URL    also run the same stories against a real OpenSearch and compare. This is
#                      what separates "we are wrong" from "this story does not pass against
#                      OpenSearch either", which happens when a story depends on something out of
#                      scope or on a different server version.
#   --spec-repo PATH   opensearch-api-specification checkout (default ../opensearch-api-specification)
#   --all              run every story in one pass instead of the manifest. Expect large numbers of
#                      failures: everything unimplemented answers 501. A to-do list, not a gate.
#
# Anything after -- is passed through to the tester, e.g. --verbose or --coverage-report.
#
# Typical development setup, with a real OpenSearch on 9200:
#   ./gradlew :server:run --args="--port 9400 --data-path /tmp/cp-conformance"
#   conformance/run.sh --reference http://localhost:9200

set -euo pipefail

URL="http://localhost:9400"
REFERENCE=""
SPEC_REPO=""
RUN_ALL=0
EXTRA_ARGS=()

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "$script_dir/.." && pwd)"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --url) URL="$2"; shift 2 ;;
    --reference) REFERENCE="$2"; shift 2 ;;
    --spec-repo) SPEC_REPO="$2"; shift 2 ;;
    --all) RUN_ALL=1; shift ;;
    --) shift; EXTRA_ARGS=("$@"); break ;;
    -h|--help) sed -n '7,30p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'; exit 0 ;;
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

require_reachable() {
  local url="$1" what="$2" code
  # Any HTTP status means reachable. Deliberately not `curl -f`: the control plane answers 501 for
  # unimplemented operations and 404 for an empty pipeline listing, both of which are correct and
  # neither of which means it is down.
  code="$(curl -sS -o /dev/null -m 5 -w '%{http_code}' "$url" 2>/dev/null || true)"
  if [[ -n "$code" && "$code" != "000" ]]; then
    return 0
  fi
  cat >&2 <<EOF
error: nothing answered HTTP at $url ($what)

If this is the control plane, start it on a port that does not collide with a real OpenSearch:

  ./gradlew :server:run --args="--port 9400 --data-path /tmp/cp-conformance"

A throwaway --data-path is worth using: the harness creates and deletes real resources.
EOF
  exit 1
}

require_reachable "$URL" "control plane"
[[ -n "$REFERENCE" ]] && require_reachable "$REFERENCE" "reference OpenSearch"

# The tester calls wait_until_available() before any story runs: it GETs / and reads
# version.number off the response. If `info` is unimplemented the 501 is retried 20 times at 5s
# intervals before failing, which looks like a hang. Fail fast with an explanation instead.
info_code="$(curl -sS -o /dev/null -m 5 -w '%{http_code}' "$URL/" 2>/dev/null || true)"
if [[ "$info_code" != "200" ]]; then
  cat >&2 <<EOF
error: GET $URL/ returned $info_code, but the harness requires it to return 200

The tester reads version.number from GET / before running any story, and retries for 100 seconds
before giving up. The 'info' operation group has to be implemented for conformance testing to work
at all -- it is the gate for everything else.
EOF
  exit 1
fi

if [[ -n "$REFERENCE" && "$REFERENCE" == "$URL" ]]; then
  echo "error: --reference is the same URL as --url; nothing to compare" >&2
  exit 2
fi

run_tester() {
  # ${arr[@]+...} guards against set -u treating an empty array as unbound, which bash 3.2 (the
  # macOS default) does.
  (cd "$SPEC_REPO" && npx ts-node tools/src/tester/test.ts \
    --spec "$SPEC_REPO/spec" \
    --tests-path "$1" \
    --opensearch-url "$2" \
    ${EXTRA_ARGS[@]+"${EXTRA_ARGS[@]}"})
}

# --- run -------------------------------------------------------------------

if [[ "$RUN_ALL" == "1" ]]; then
  echo "Running ALL stories against $URL (expect 501 failures for unimplemented operations)"
  run_tester "$SPEC_REPO/tests" "$URL"
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

echo "Running ${#stories[@]} story file(s) from $manifest"
echo "  control plane: $URL"
[[ -n "$REFERENCE" ]] && echo "  reference:     $REFERENCE"
echo

cp_failed=()
ref_failed=()
for story in "${stories[@]}"; do
  echo "=== $story  [control plane]"
  run_tester "$SPEC_REPO/tests/$story" "$URL" || cp_failed+=("$story")
  echo

  if [[ -n "$REFERENCE" ]]; then
    echo "=== $story  [reference]"
    run_tester "$SPEC_REPO/tests/$story" "$REFERENCE" || ref_failed+=("$story")
    echo
  fi
done

contains() {
  local needle="$1"; shift
  local item
  for item in "$@"; do [[ "$item" == "$needle" ]] && return 0; done
  return 1
}

echo "──────────────────────────────────────────────"

if [[ -z "$REFERENCE" ]]; then
  if [[ "${#cp_failed[@]}" == "0" ]]; then
    echo "conformance: ${#stories[@]}/${#stories[@]} stories passed"
    exit 0
  fi
  echo "conformance: $((${#stories[@]} - ${#cp_failed[@]}))/${#stories[@]} passed, ${#cp_failed[@]} failed:"
  for story in "${cp_failed[@]}"; do echo "  - $story"; done
  exit 1
fi

# With a reference, a story failing on both sides is not evidence against us -- it usually means the
# story reaches something out of scope, or the reference is a different OpenSearch version. Only a
# story that passes against OpenSearch and fails against us is a divergence.
divergent=()
both_failed=()
for story in "${stories[@]}"; do
  cp_ok=1; ref_ok=1
  contains "$story" ${cp_failed[@]+"${cp_failed[@]}"} && cp_ok=0
  contains "$story" ${ref_failed[@]+"${ref_failed[@]}"} && ref_ok=0

  if [[ "$cp_ok" == "1" && "$ref_ok" == "1" ]]; then verdict="agree (both pass)"
  elif [[ "$cp_ok" == "0" && "$ref_ok" == "1" ]]; then verdict="DIVERGENT"; divergent+=("$story")
  elif [[ "$cp_ok" == "1" && "$ref_ok" == "0" ]]; then verdict="passes here, fails on reference"
  else verdict="fails on both (story or scope issue, not a divergence)"; both_failed+=("$story")
  fi
  printf '  %-44s %s\n' "$story" "$verdict"
done

echo
if [[ "${#divergent[@]}" == "0" ]]; then
  echo "conformance: no divergences from the reference"
  [[ "${#both_failed[@]}" != "0" ]] && echo "note: ${#both_failed[@]} story/stories fail on both sides; check scope or reference version"
  exit 0
fi
echo "conformance: ${#divergent[@]} divergence(s) -- passes against OpenSearch, fails here:"
for story in "${divergent[@]}"; do echo "  - $story"; done
exit 1
