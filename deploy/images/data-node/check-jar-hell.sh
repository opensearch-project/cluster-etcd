#!/usr/bin/env bash
#
# check-jar-hell.sh <node lib dir> <plugin dir>
#
# Fails if a class in the plugin's jars is also in the node's, or in two of the plugin's own jars: what
# OpenSearch's JarHell refuses when it loads the plugin. By class rather than by jar name, because a library
# can be present twice under two names, and the same name can be two libraries (Jackson 2 and Jackson 3).
set -euo pipefail
export LC_ALL=C

classes() {
  for jar in "$1"/*.jar; do
    # A jar with no classes (Guava's listenablefuture placeholder) is not an error.
    unzip -Z1 "$jar" | { grep '\.class$' || true; } | { grep -v -e '^META-INF/' -e 'module-info\.class$' || true; } \
      | sed "s|\$| $(basename "$jar")|"
  done
}

core="$(mktemp)"; plugin="$(mktemp)"
trap 'rm -f "$core" "$plugin"' EXIT
classes "$1" | sort -k1,1 > "$core"
classes "$2" | sort -k1,1 > "$plugin"

status=0
clashes="$(join "$plugin" "$core" | awk '{ print $2 " duplicates " $3 }' | sort -u)"
if [ -n "$clashes" ]; then
  echo "jar hell: these plugin jars duplicate classes in the node's lib/:" >&2
  echo "$clashes" >&2
  status=1
fi
# `|| true`: head closes the pipe after 20 lines, and under pipefail uniq's SIGPIPE would otherwise end the script
# with 141 before saying why, precisely when there are many duplicates.
within="$(awk '{ print $1 }' "$plugin" | uniq -d | head -20 || true)"
if [ -n "$within" ]; then
  echo "jar hell: classes present in two of the plugin's own jars, for example:" >&2
  for class in $within; do grep "^$class " "$plugin" >&2; done
  status=1
fi
exit "$status"
