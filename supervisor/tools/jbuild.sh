#!/usr/bin/env bash
#
# Compile and test without Gradle.
#
# Gradle cannot run in the development sandbox: it fails creating its file-lock service, because binding a
# socket is denied. Every dependency this build needs is already in ~/.gradle/caches or ~/.m2, so javac and
# a hand-rolled JUnit launcher get the same work done. See WORKING_NOTES.md.
#
# THIS IS NOT THE BUILD. ./gradlew build is, and it is unverified until a human runs it. Anything this
# script has to special-case is a difference between the two, and the difference is the bug.
#
# Usage:
#   tools/jbuild.sh                 compile everything and run every test
#   tools/jbuild.sh --no-tests      compile only
#   tools/jbuild.sh KeysTests       run only tests whose class name matches (regex, anchored by JUnit)
#
set -euo pipefail

cd "$(dirname "$0")/.."
ROOT="$PWD"
KV_STORE="${KV_STORE:-$ROOT/../opensearch-kv-store}"
OUT="$ROOT/build/jbuild"

# Matches the Gradle toolchain. The installed JDK is newer, so --release keeps the two honest: code that
# compiles here but not under a real 21 toolchain is a difference we want to find now.
RELEASE=21

JACKSON_VERSION=2.22.1
# jackson-annotations tracks the minor, not the patch: databind 2.22.1 depends on annotations 2.22.
JACKSON_ANNOTATIONS_VERSION=2.22
JUNIT_JUPITER_VERSION=5.11.4
JUNIT_PLATFORM_VERSION=1.11.4

RUN_TESTS=1
TEST_PATTERNS=()
for arg in "$@"; do
    case "$arg" in
        --no-tests) RUN_TESTS=0 ;;
        -h|--help) sed -n '2,20p' "$0"; exit 0 ;;
        *) TEST_PATTERNS+=(".*${arg}.*") ;;
    esac
done

# ---------------------------------------------------------------- dependency resolution
#
# Gradle's cache lays artifacts out as files-2.1/<group>/<artifact>/<version>/<sha1>/<file>, with the sha1
# in the path, so the jar has to be found rather than constructed. Maven's layout is deterministic and is
# the fallback: WORKING_NOTES.md records that the JUnit platform jars live in ~/.m2 but not always in the
# Gradle cache.
jar() {
    local group="$1" artifact="$2" version="$3"
    local found

    found=$(find "$HOME/.gradle/caches/modules-2/files-2.1/$group/$artifact/$version" \
        -name "$artifact-$version.jar" 2>/dev/null | head -1)
    if [[ -n "$found" ]]; then
        echo "$found"
        return 0
    fi

    found="$HOME/.m2/repository/${group//.//}/$artifact/$version/$artifact-$version.jar"
    if [[ -f "$found" ]]; then
        echo "$found"
        return 0
    fi

    echo "MISSING: $group:$artifact:$version" >&2
    echo "  Maven Central is unreachable from here, so a dependency not already in a local cache cannot" >&2
    echo "  be fetched and this module cannot be compiled. Ask a human to run ./gradlew build." >&2
    return 1
}

join_cp() { local IFS=:; echo "$*"; }

echo "==> resolving dependencies"
JACKSON_DATABIND=$(jar com.fasterxml.jackson.core jackson-databind "$JACKSON_VERSION")
JACKSON_CORE=$(jar com.fasterxml.jackson.core jackson-core "$JACKSON_VERSION")
JACKSON_ANNOTATIONS=$(jar com.fasterxml.jackson.core jackson-annotations "$JACKSON_ANNOTATIONS_VERSION")
JUPITER_API=$(jar org.junit.jupiter junit-jupiter-api "$JUNIT_JUPITER_VERSION")
JUPITER_ENGINE=$(jar org.junit.jupiter junit-jupiter-engine "$JUNIT_JUPITER_VERSION")
JUPITER_PARAMS=$(jar org.junit.jupiter junit-jupiter-params "$JUNIT_JUPITER_VERSION")
PLATFORM_COMMONS=$(jar org.junit.platform junit-platform-commons "$JUNIT_PLATFORM_VERSION")
PLATFORM_ENGINE=$(jar org.junit.platform junit-platform-engine "$JUNIT_PLATFORM_VERSION")
PLATFORM_LAUNCHER=$(jar org.junit.platform junit-platform-launcher "$JUNIT_PLATFORM_VERSION")
OPENTEST4J=$(jar org.opentest4j opentest4j 1.3.0)
APIGUARDIAN=$(jar org.apiguardian apiguardian-api 1.1.2)

JACKSON_CP=$(join_cp "$JACKSON_DATABIND" "$JACKSON_CORE" "$JACKSON_ANNOTATIONS")
JUNIT_CP=$(join_cp "$JUPITER_API" "$JUPITER_ENGINE" "$JUPITER_PARAMS" \
    "$PLATFORM_COMMONS" "$PLATFORM_ENGINE" "$PLATFORM_LAUNCHER" "$OPENTEST4J" "$APIGUARDIAN")

# ---------------------------------------------------------------- compilation

compile() {
    local name="$1" src="$2" dest="$3" cp="${4:-}"
    local sources
    sources=$(find "$src" -name '*.java' 2>/dev/null || true)
    if [[ -z "$sources" ]]; then
        echo "    $name: no sources, skipped"
        return 0
    fi
    mkdir -p "$dest"
    # -Xlint:all matches the Gradle build, minus the two suppressions it also applies.
    # shellcheck disable=SC2086
    javac --release "$RELEASE" -encoding UTF-8 -Xlint:all -Xlint:-serial -Xlint:-this-escape \
        ${cp:+-cp "$cp"} -d "$dest" $sources
    echo "    $name: $(echo "$sources" | wc -l | tr -d ' ') sources -> $dest"
}

rm -rf "$OUT"

# store-api is compiled from source rather than taken from ../opensearch-kv-store/build/libs, so that a
# change over there is picked up here immediately. It has no dependencies at all, which makes this cheap.
echo "==> store-api (from source: $KV_STORE)"
if [[ ! -d "$KV_STORE/store-api/src/main/java" ]]; then
    echo "  store-api sources not found at $KV_STORE" >&2
    echo "  Set KV_STORE=/path/to/opensearch-kv-store, or check out the repository beside this one." >&2
    exit 1
fi
compile "store-api main" "$KV_STORE/store-api/src/main/java" "$OUT/store-api/main"
compile "store-api testFixtures" "$KV_STORE/store-api/src/testFixtures/java" "$OUT/store-api/testFixtures" \
    "$OUT/store-api/main:$JUNIT_CP"

STORE_CP="$OUT/store-api/main"
STORE_FIXTURES_CP="$OUT/store-api/main:$OUT/store-api/testFixtures"

echo "==> main sources"
compile "supervisor-model" supervisor-model/src/main/java "$OUT/supervisor-model/main" "$JACKSON_CP"
compile "supervisor-core" supervisor-core/src/main/java "$OUT/supervisor-core/main" \
    "$OUT/supervisor-model/main:$STORE_CP:$JACKSON_CP"
compile "supervisor-app" supervisor-app/src/main/java "$OUT/supervisor-app/main" \
    "$OUT/supervisor-core/main:$OUT/supervisor-model/main:$STORE_CP:$JACKSON_CP"
compile "fake-node" fake-node/src/main/java "$OUT/fake-node/main" \
    "$OUT/supervisor-model/main:$STORE_CP:$JACKSON_CP"

# supervisor-model must not be able to see the store. DESIGN.md §1 claims the diff is a pure function of an
# observation, and the module graph is what makes that checkable rather than a matter of review discipline.
# Gradle enforces it through the dependency declarations; here it has to be asserted.
echo "==> checking the module graph"
if grep -rq "org.opensearch.controlplane.store" supervisor-model/src 2>/dev/null; then
    echo "  supervisor-model references the store API, which breaks the purity claim in DESIGN.md §1" >&2
    exit 1
fi
echo "    supervisor-model does not reference store-api"

if [[ $RUN_TESTS -eq 0 ]]; then
    echo "==> done (--no-tests)"
    exit 0
fi

echo "==> test sources"
compile "supervisor-model tests" supervisor-model/src/test/java "$OUT/supervisor-model/test" \
    "$OUT/supervisor-model/main:$JACKSON_CP:$JUNIT_CP"
compile "supervisor-core tests" supervisor-core/src/test/java "$OUT/supervisor-core/test" \
    "$OUT/supervisor-core/main:$OUT/supervisor-model/main:$STORE_FIXTURES_CP:$JACKSON_CP:$JUNIT_CP"
compile "supervisor-app tests" supervisor-app/src/test/java "$OUT/supervisor-app/test" \
    "$OUT/supervisor-app/main:$OUT/supervisor-core/main:$OUT/supervisor-model/main:$STORE_CP:$JACKSON_CP:$JUNIT_CP"
compile "fake-node tests" fake-node/src/test/java "$OUT/fake-node/test" \
    "$OUT/fake-node/main:$OUT/supervisor-model/main:$STORE_CP:$JACKSON_CP:$JUNIT_CP"

echo "==> test runner"
compile "JUnitRunner" tools "$OUT/tools" "$JUNIT_CP"

echo "==> running tests"
TEST_CP=$(join_cp \
    "$OUT/tools" \
    "$OUT/supervisor-model/main" "$OUT/supervisor-model/test" \
    "$OUT/supervisor-core/main" "$OUT/supervisor-core/test" \
    "$OUT/supervisor-app/main" "$OUT/supervisor-app/test" \
    "$OUT/fake-node/main" "$OUT/fake-node/test" \
    "$STORE_FIXTURES_CP" "$JACKSON_CP" "$JUNIT_CP")

ROOTS=()
for candidate in "$OUT"/*/test; do
    [[ -d "$candidate" ]] && ROOTS+=("$candidate")
done

if [[ ${#TEST_PATTERNS[@]} -gt 0 ]]; then
    java -cp "$TEST_CP" tools.JUnitRunner "${ROOTS[@]}" -- "${TEST_PATTERNS[@]}"
else
    java -cp "$TEST_CP" tools.JUnitRunner "${ROOTS[@]}"
fi
