#!/usr/bin/env bash
#
# Copyright OpenSearch Software Foundation
# SPDX-License-Identifier: Apache-2.0
#
# Emits a control plane capabilities file describing what a given OpenSearch cluster supports.
#
#   tools/dump-capabilities.sh http://localhost:9200 [http://localhost:9201 ...] > capabilities.yaml
#   ../gradlew :server:run --args="--capabilities capabilities.yaml"
#
# Give it every data node, or at least one of each version running. A clusterless node reports only itself
# under GET /_nodes, so the minimum node version and the processor intersection are only as good as the set of
# nodes named here. The script-only sections come from the first URL.
#
# Generated rather than hand-written on purpose. The processor types a cluster has depend on which
# plugins are installed, nothing in the OpenSearch API specification records that, and a list
# maintained by hand would drift -- causing the control plane to reject definitions that would
# actually have worked.
#
# Four sources, because a cluster reports these capabilities in four places:
#   GET /_nodes                   processor types, intersected below, and each data node version
#   GET /_script_language         scripting languages and their contexts
#   GET /_script_context          scripting contexts and their method signatures
#   GET /_ingest/processor/grok   the built-in grok pattern library
#
# The last three are also operations the control plane serves, and for those the capability data
# *is* the response -- there is nothing to compute from it. So each is emitted only when its query
# succeeds, and the control plane registers the matching operation only when the section is present.
# Without it the operation keeps its 501, which is an honest answer where an empty list would be a
# confident wrong one. GET /_script_language additionally lets stored scripts have their lang
# validated; the other two validate nothing.
#
# Expect a large file. The grok library alone is several hundred patterns and _script_context runs to
# a few hundred method signatures, so a full dump is a few thousand lines against roughly sixty for
# processors alone. It is generated, and diffing it is how you see a data plane change.
#
# Run this against the cluster this control plane fronts, and re-run it when plugins change.
#
# MAINTENANCE: the Python below is one single-quoted shell argument, so it must contain no apostrophe
# anywhere -- not even in a comment or a docstring. One turns the rest of the script into shell code
# and the failure is a bash syntax error pointing at an innocent line. `bash -n` on this file catches
# it; run that after editing.

set -euo pipefail

if [ "$#" -eq 0 ]; then
  set -- http://localhost:9200
fi
URL="$1"

if command -v python3 >/dev/null 2>&1; then :; else
  echo "error: python3 is required" >&2; exit 1
fi

# One _nodes response per URL, merged below by node id, so naming a node twice, or two URLs that report the
# same node, counts it once.
node_responses=""
for url in "$@"; do
  response="$(curl -fsS -m 30 "$url/_nodes?filter_path=nodes.*.ingest.processors.type,nodes.*.search_pipelines.*.type,nodes.*.version,nodes.*.roles")" || {
    echo "error: could not query $url/_nodes" >&2
    exit 1
  }
  node_responses="$node_responses$response"$'\n'
done

# None of these three is fatal: an older cluster, or one behind a proxy that blocks it, simply yields
# no section rather than a broken file.
languages_json="$(curl -fsS -m 30 "$URL/_script_language" 2>/dev/null || true)"
contexts_json="$(curl -fsS -m 30 "$URL/_script_context" 2>/dev/null || true)"
grok_json="$(curl -fsS -m 30 "$URL/_ingest/processor/grok" 2>/dev/null || true)"

# One response per line. OpenSearch escapes newlines inside JSON strings, so a response never spans
# lines and this stays unambiguous -- which matters for grok patterns, whose values are dense with
# backslashes.
{ printf '%s\n' "$#"; printf '%s' "$node_responses"; printf '%s\n%s\n%s\n' \
  "${languages_json:-}" "${contexts_json:-}" "${grok_json:-}"; } | python3 -c '
import json, sys

lines = sys.stdin.read().split("\n")
node_urls = int(lines[0])
node_lines = lines[1:1 + node_urls]
lines = lines[1 + node_urls:]

def optional(index):
    """A response that is allowed to be missing or unparseable; absent means "no section"."""
    if index >= len(lines) or not lines[index].strip():
        return None
    try:
        return json.loads(lines[index])
    except json.JSONDecodeError:
        return None

nodes = {}
for line in node_lines:
    try:
        nodes.update(json.loads(line).get("nodes") or {})
    except json.JSONDecodeError as e:
        sys.exit(f"error: could not parse a _nodes response ({e})")
if not nodes:
    sys.exit("error: the _nodes responses contained no nodes")

languages = optional(0)
contexts = optional(1)
grok = optional(2)


def version_key(text):
    # 3.10.0-SNAPSHOT orders as 3.10.0: compared by number, never as text, where 3.9.0 would sort after 3.10.0.
    return tuple(int(part) for part in text.split("-")[0].split("."))

data_versions = [n["version"] for n in nodes.values() if "data" in (n.get("roles") or []) and n.get("version")]
minimum_version = min(data_versions, key=version_key).split("-")[0] if data_versions else None

# Intersect across nodes rather than union: a processor only usable on some nodes is not something
# the control plane should accept. A homogeneous cluster makes this a no-op.
kinds = {
    "ingest": ("ingest", "processors"),
    "search_request": ("search_pipelines", "request_processors"),
    "search_response": ("search_pipelines", "response_processors"),
    "search_phase_results": ("search_pipelines", "phase_results_processors"),
}

result, node_count = {}, len(nodes)
for key, (section, field) in kinds.items():
    per_node = []
    for node in nodes.values():
        entries = node.get(section, {}).get(field, []) or []
        per_node.append({e["type"] for e in entries if "type" in e})
    result[key] = sorted(set.intersection(*per_node)) if per_node else []


def scalar(value):
    """A YAML double-quoted scalar.

    json.dumps is used rather than bare text on purpose. Grok patterns are regexes full of
    backslashes, quotes, colons and leading hashes, and return types carry things like
    java.util.Map<String,Object> -- all of which are ambiguous or invalid as plain YAML scalars. A
    JSON string is a valid YAML 1.2 double-quoted scalar, escape for escape, so this is exact rather
    than merely careful.
    """
    return json.dumps("" if value is None else str(value))


print("# Capabilities of the data plane behind this control plane.")
print("#")
print(f"# Generated by tools/dump-capabilities.sh from {node_count} node(s).")
print("# Do not edit by hand -- re-run the script when the data plane changes.")
print("#")
print("# Types present on only some nodes are excluded: the control plane should not accept a")
print("# definition that would fail depending on where it ran.")
print("processors:")
for key in kinds:
    types = result[key]
    if not types:
        print(f"  {key}: []")
    else:
        print(f"  {key}:")
        for t in types:
            print(f"    - {t}")

if minimum_version:
    print()
    print("# The oldest version among the data nodes queried. A new index is created at this version, as")
    print("# OpenSearch fixes index.version.created to the smallest node version, so it must cover every data")
    print("# node that could hold one: an index created newer than a node cannot be held there. Re-run after")
    print("# an upgrade completes. Lower than the truth is safe; higher is not.")
    print(f"minimum_node_version: {scalar(minimum_version)}")

if languages:
    print()
    print("# From GET /_script_language. With this present the control plane serves")
    print("# get_script_languages and validates the lang of a stored script; without it, both degrade.")
    print("script_languages:")
    types_allowed = languages.get("types_allowed") or []
    print("  types_allowed: [" + ", ".join(types_allowed) + "]")
    language_contexts = languages.get("language_contexts") or []
    if not language_contexts:
        print("  language_contexts: {}")
    else:
        print("  language_contexts:")
        for entry in sorted(language_contexts, key=lambda e: e.get("language", "")):
            names = sorted(entry.get("contexts") or [])
            print("    " + entry.get("language", "") + ":")
            for name in names:
                print(f"      - {name}")

if contexts:
    entries = contexts.get("contexts") or []
    print()
    print("# From GET /_script_context. A pure passthrough: the control plane serves")
    print("# get_script_context from this and validates nothing with it, so without this section the")
    print("# operation keeps its 501 rather than claiming a cluster has no scripting contexts.")
    print("#")
    print("# Order is preserved exactly as the endpoint returned it, and is NOT sorted here. It only")
    print("# looks alphabetical: contexts are sorted by name upstream, but within a context")
    print("# ScriptContextInfo.toXContent emits execute FIRST and then the getters sorted by name. A")
    print("# getter sorting before the word execute -- anything starting a-d -- would come out in the")
    print("# wrong place if this file re-sorted, and the response would stop being byte-exact. Both")
    print("# orders are deterministic for a given cluster, so a re-dump is still a no-op diff.")
    if not entries:
        print("script_contexts: []")
    else:
        print("script_contexts:")
        for context in entries:
            print("  - name: " + scalar(context.get("name")))
            methods = context.get("methods") or []
            if not methods:
                print("    methods: []")
                continue
            print("    methods:")
            for method in methods:
                print("      - name: " + scalar(method.get("name")))
                print("        return_type: " + scalar(method.get("return_type")))
                params = method.get("params") or []
                if not params:
                    print("        params: []")
                    continue
                # Parameter order is the signature.
                print("        params:")
                for param in params:
                    print("          - name: " + scalar(param.get("name")))
                    print("            type: " + scalar(param.get("type")))

if grok:
    patterns = grok.get("patterns") or {}
    print()
    print("# From GET /_ingest/processor/grok. The other pure passthrough, and the reason this file is")
    print("# large. Which patterns exist is a property of the ingest-common module, so it is a data")
    print("# plane fact like any other rather than something to hardcode here.")
    print("#")
    print("# Order is the pattern-file load order and is NOT sorted here, because the endpoint has two")
    print("# modes: the default answers Grok.BUILTIN_PATTERNS, a LinkedHashMap in load order, and ?s=true")
    print("# answers a TreeMap of the same thing. Keeping load order lets the control plane reproduce")
    print("# both -- it can sort on demand but could not unsort. Load order is deterministic for a")
    print("# given version, so a re-dump is still a no-op diff.")
    print("#")
    print("# Values are YAML double-quoted scalars because grok patterns are regexes: backslashes,")
    print("# quotes and colons all appear, and several patterns start with a character that would")
    print("# change meaning unquoted.")
    if not patterns:
        print("grok_patterns: {}")
    else:
        print("grok_patterns:")
        for name, pattern in patterns.items():
            print(f"  {scalar(name)}: {scalar(pattern)}")
'
