#!/usr/bin/env bash
#
# Compile and test without Gradle.
#
# Gradle cannot run in the development sandbox: it fails creating its file-lock service, because binding a
# socket is denied. Every dependency this build needs is already on disk — the OpenSearch server jar in a
# sibling checkout, everything else in ~/.gradle/caches or ~/.m2 — so javac and a hand-rolled JUnit
# launcher get the same work done. The supervisor repository next door has the same script for the same
# reason; this one differs only in having to find OpenSearch.
#
# THIS IS NOT THE BUILD. ./gradlew build is, and it additionally runs the OpenSearch plugin packaging,
# forbidden-APIs and the license checks, none of which are reproduced here. Anything this script has to
# special-case is a difference between the two, and the difference is the bug.
#
# Usage:
#   tools/jbuild.sh                 compile everything and run every test
#   tools/jbuild.sh --no-tests      compile only
#   tools/jbuild.sh NodePublisher   run only tests whose class name matches (regex, anchored by JUnit)
#
set -euo pipefail

cd "$(dirname "$0")/.."
ROOT="$PWD"
KV_STORE="${KV_STORE:-$ROOT/../opensearch-kv-store}"
SUPERVISOR="${SUPERVISOR:-$ROOT/../opensearch-supervisor}"
OPENSEARCH="${OPENSEARCH:-$ROOT/../OpenSearch}"
OUT="$ROOT/build/jbuild"

RELEASE=21

JAVA21_HOME="${JAVA21_HOME:-}"
if [[ -z "$JAVA21_HOME" && -x /usr/libexec/java_home ]]; then
    JAVA21_HOME=$(/usr/libexec/java_home -v "$RELEASE" 2>/dev/null || true)
fi
if [[ -n "$JAVA21_HOME" && -x "$JAVA21_HOME/bin/javac" ]]; then
    JAVAC="$JAVA21_HOME/bin/javac"
    JAVA="$JAVA21_HOME/bin/java"
else
    JAVAC=javac
    JAVA=java
    echo "WARNING: no Java $RELEASE toolchain found; falling back to $(javac -version 2>&1) with --release $RELEASE." >&2
fi

# Matches the supervisor, so the conformance fixtures compile against what they were written for.
JACKSON_VERSION=2.22.1
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
    echo "  Maven Central is unreachable from the sandbox, so a dependency not already in a local cache" >&2
    echo "  cannot be fetched. Ask a human to run ./gradlew build." >&2
    return 1
}

join_cp() { local IFS=:; echo "$*"; }

# The OpenSearch API this plugin compiles against.
#
# From the sibling checkout's build output rather than from a published artifact, because that is the
# version the Gradle build targets and because having the sources beside the jars is the difference
# between reading what a server method does and guessing. ../OpenSearch must have been built once:
#
#     cd ../OpenSearch && ./gradlew :server:jar :libs:core:jar :libs:common:jar :libs:x-content:jar
#
echo "==> resolving OpenSearch from $OPENSEARCH"
# server plus every built lib, rather than the four the plugin's own imports need. Static initialisers
# reach much further than compilation does: ClusterSettings' <clinit> touches the telemetry lib, so a
# classpath trimmed to what javac wanted compiled fine and then failed at runtime with
# NoClassDefFoundError inside a class this plugin only mentions in a test. The plugin's real classpath is
# the whole distribution; approximating it more closely is cheaper than discovering the next one.
#
# Searched recursively, not with libs/*/: some modules are nested a level deeper -- agent-policy, which
# BootstrapForTesting needs, lives at libs/agent-sm/agent-policy.
OPENSEARCH_JARS=()
SERVER_JAR=$(find "$OPENSEARCH/server/build/distributions" -name '*.jar' ! -name '*-sources.jar' 2>/dev/null | head -1)
if [[ -z "$SERVER_JAR" ]]; then
    echo "  MISSING: $OPENSEARCH/server/build/distributions/*.jar" >&2
    echo "  Build it once:  cd $OPENSEARCH && ./gradlew assemble" >&2
    echo "  Or set OPENSEARCH=/path/to/OpenSearch." >&2
    exit 1
fi
OPENSEARCH_JARS+=("$SERVER_JAR")
echo "    $(basename "$SERVER_JAR")"
# Deduplicated by file name, and this is not paranoia: libs/agent-sm/agent and libs/agent-sm/bootstrap both
# produce opensearch-agent-bootstrap-<version>.jar carrying the same classes, because the agent bundles the
# bootstrap library. OpenSearchTestCase's static initialiser runs JarHell.checkJarHell over the whole
# classpath and refuses to start when two jars share a class -- so a duplicate here is not a warning, it is
# every test in the suite failing to initialise. The library is what belongs on a classpath; the agent is
# what belongs on -javaagent.
# A newline-delimited string rather than an associative array: macOS ships bash 3.2, where `declare -A` does
# not exist, and this script has to run on the machine it is written on.
SEEN_JARS=""
while IFS= read -r found; do
    name=$(basename "$found")
    case "$SEEN_JARS" in
        *"|$name|"*) continue ;;
    esac
    if [[ "$found" == */libs/agent-sm/agent/* ]]; then
        continue
    fi
    SEEN_JARS="$SEEN_JARS|$name|"
    OPENSEARCH_JARS+=("$found")
done < <(find "$OPENSEARCH/libs" -path '*/build/distributions/*.jar' ! -name '*-sources.jar' ! -name '*-javadoc.jar' \
    2>/dev/null | sort)
echo "    and ${#OPENSEARCH_JARS[@]} jars in total, from server and libs/**"

echo "==> resolving dependencies"
# Referenced from OpenSearch signatures this plugin touches, so javac needs them even though no code here
# imports them.
LOG4J_API=$(jar org.apache.logging.log4j log4j-api 2.25.4)

# Lucene, at the version ../OpenSearch pins rather than the newest one cached -- several may be,
# and picking the wrong one would compile and then fail somewhere specific and confusing. Every artifact,
# not just the core, because ClusterSettings' static initialiser reaches into the analysis package.
LUCENE_VERSION=$(sed -n 's/^lucene *= *"\(.*\)"/\1/p' "$OPENSEARCH/gradle/libs.versions.toml" | head -1)
if [[ -z "$LUCENE_VERSION" ]]; then
    echo "  could not read the lucene version from $OPENSEARCH/gradle/libs.versions.toml" >&2
    exit 1
fi
LUCENE_JARS=$(find "$HOME/.gradle/caches/modules-2/files-2.1/org.apache.lucene" \
    -name "*-$LUCENE_VERSION.jar" ! -name '*-sources.jar' 2>/dev/null | sort | tr '\n' ':')
if [[ -z "$LUCENE_JARS" ]]; then
    echo "  MISSING: org.apache.lucene:*:$LUCENE_VERSION in a local cache" >&2
    exit 1
fi
echo "    lucene $LUCENE_VERSION"
# OpenSearch 3.9's XContent is built on Jackson 3 (tools.jackson.*), which is a different artifact from
# the Jackson 2 the supervisor's codec uses -- so both are on the test classpath at once, under different
# package names, and neither shadows the other. Runtime only: nothing here compiles against either.
JACKSON3_CORE=$(jar tools.jackson.core jackson-core 3.2.2)
JACKSON_DATABIND=$(jar com.fasterxml.jackson.core jackson-databind "$JACKSON_VERSION")
JACKSON_CORE=$(jar com.fasterxml.jackson.core jackson-core "$JACKSON_VERSION")
JACKSON_ANNOTATIONS=$(jar com.fasterxml.jackson.core jackson-annotations "$JACKSON_ANNOTATIONS_VERSION")
# The JUnit 4 stack the OpenSearch plugin build runs, and that OpenSearchTestCase needs. The plugin's own
# tests are JUnit 4 because the real build is: nothing in the OpenSearch build calls useJUnitPlatform(), and
# testingConventions expects a unit test to extend LuceneTestCase.
JUNIT4=$(jar junit junit 4.13.2)
# OpenSearchTestCase captures log output, so it references log4j-core and not only the api the plugin uses.
LOG4J_CORE=$(jar org.apache.logging.log4j log4j-core 2.25.4)
# Reached from OpenSearchTestCase's inherited signatures once RandomizedRunner starts instantiating things.
JODA_TIME=$(jar joda-time joda-time 2.12.7)
# BootstrapForTesting calls Bootstrap.initializeNatives, which reaches JNA even on a platform where the
# Windows entry points it mentions are never called.
JNA=$(jar net.java.dev.jna jna 5.16.0)
# OpenSearchTestCase.tearDown drains reactor's schedulers, so it needs reactor even for a test that never
# touches one. Also reactive-streams, which reactor's own API mentions.
REACTOR_CORE=$(jar io.projectreactor reactor-core 3.8.7)
REACTIVE_STREAMS=$(jar org.reactivestreams reactive-streams 1.0.4)
# ClusterSettings' static initialiser registers the compression codecs, which reach zstd-jni.
ZSTD=$(jar com.github.luben zstd-jni 1.5.6-1)
HAMCREST=$(jar org.hamcrest hamcrest 2.2)
RANDOMIZED_RUNNER=$(jar com.carrotsearch.randomizedtesting randomizedtesting-runner 2.7.1)
OPENSEARCH_TEST_FRAMEWORK=$(find "$OPENSEARCH/test/framework/build/distributions" -name 'framework-*.jar' \
    ! -name '*-sources.jar' ! -name '*-javadoc.jar' 2>/dev/null | head -1)
if [[ -z "$OPENSEARCH_TEST_FRAMEWORK" ]]; then
    echo "  MISSING: $OPENSEARCH/test/framework/build/distributions/framework-*.jar" >&2
    echo "  Build it once:  cd $OPENSEARCH && ./gradlew :test:framework:jar" >&2
    exit 1
fi

# jupiter-api stays, as a library rather than a framework: the shared node-contract checks in the supervisor
# repository assert with org.junit.jupiter.api.Assertions, which throws opentest4j errors that JUnit 4
# reports perfectly well. No jupiter engine is on this classpath and none is wanted.
JUPITER_API=$(jar org.junit.jupiter junit-jupiter-api "$JUNIT_JUPITER_VERSION")
JUPITER_ENGINE=$(jar org.junit.jupiter junit-jupiter-engine "$JUNIT_JUPITER_VERSION")
JUPITER_PARAMS=$(jar org.junit.jupiter junit-jupiter-params "$JUNIT_JUPITER_VERSION")
PLATFORM_COMMONS=$(jar org.junit.platform junit-platform-commons "$JUNIT_PLATFORM_VERSION")
PLATFORM_ENGINE=$(jar org.junit.platform junit-platform-engine "$JUNIT_PLATFORM_VERSION")
PLATFORM_LAUNCHER=$(jar org.junit.platform junit-platform-launcher "$JUNIT_PLATFORM_VERSION")
OPENTEST4J=$(jar org.opentest4j opentest4j 1.3.0)
APIGUARDIAN=$(jar org.apiguardian apiguardian-api 1.1.2)

OPENSEARCH_CP=$(join_cp "${OPENSEARCH_JARS[@]}" "$LOG4J_API" "${LUCENE_JARS%:}")
OPENSEARCH_RUNTIME_CP=$(join_cp "$OPENSEARCH_CP" "$JACKSON3_CORE")
JACKSON_CP=$(join_cp "$JACKSON_DATABIND" "$JACKSON_CORE" "$JACKSON_ANNOTATIONS")
# What the supervisor's fixtures compile against, and what runs them here.
JUNIT_CP=$(join_cp "$JUNIT4" "$HAMCREST" "$RANDOMIZED_RUNNER" "$OPENSEARCH_TEST_FRAMEWORK" "$LOG4J_CORE" "$JODA_TIME" "$JNA" "$REACTOR_CORE" "$REACTIVE_STREAMS" "$ZSTD" \
    "$JUPITER_API" "$JUPITER_ENGINE" "$JUPITER_PARAMS" \
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
    # shellcheck disable=SC2086
    "$JAVAC" --release "$RELEASE" -encoding UTF-8 -Xlint:all -Xlint:-serial -Xlint:-this-escape \
        ${cp:+-cp "$cp"} -d "$dest" $sources
    echo "    $name: $(echo "$sources" | wc -l | tr -d ' ') sources -> $dest"
}

rm -rf "$OUT"

# store-api from source rather than from ../opensearch-kv-store/build/libs, so a change over there is
# picked up here immediately. It has no dependencies at all, which makes this cheap.
echo "==> store-api (from source: $KV_STORE)"
if [[ ! -d "$KV_STORE/store-api/src/main/java" ]]; then
    echo "  store-api sources not found at $KV_STORE" >&2
    exit 1
fi
compile "store-api main" "$KV_STORE/store-api/src/main/java" "$OUT/store-api/main"
# Resources matter: store-api registers its bundled backend through META-INF/services, and ServiceLoader
# finds providers on the classpath rather than by compilation. Gradle copies these as a matter of course.
if [[ -d "$KV_STORE/store-api/src/main/resources" ]]; then
    cp -R "$KV_STORE/store-api/src/main/resources/." "$OUT/store-api/main/"
fi
compile "store-api testFixtures" "$KV_STORE/store-api/src/testFixtures/java" "$OUT/store-api/testFixtures" \
    "$OUT/store-api/main:$JUNIT_CP"

STORE_CP="$OUT/store-api/main"
STORE_FIXTURES_CP="$OUT/store-api/main:$OUT/store-api/testFixtures"

echo "==> plugin"
compile "plugin main" plugin/src/main/java "$OUT/plugin/main" "$OPENSEARCH_CP:$STORE_CP"

# D6.2: nothing outside test code and the SPI wiring may name a backend. Gradle enforces the important
# half through the dependency declarations -- store-etcd and store-zookeeper are simply not on the compile
# classpath -- but a grep is what catches a string literal, which a missing dependency would not.
#
# Comments are stripped first, and that is not a loophole. The requirement is that no backend is named in
# anything that *runs*: a setting key, a class name, a scheme literal, a conditional. Explaining in prose
# why etcd is not mentioned in the code is the opposite of the problem, and a check that forbade it would
# be a check that punished writing down the reason.
echo "==> checking that no backend is named"
BACKEND_HITS=$(find plugin/src/main/java -name '*.java' -print0 \
    | xargs -0 grep -nE 'jetcd|curator|zookeeper|\betcd\b' \
    | grep -vE ':[0-9]+: *(\*|//|/\*)' || true)
if [[ -n "$BACKEND_HITS" ]]; then
    echo "  plugin/src/main names a store backend in code, which breaks D6.2:" >&2
    echo "$BACKEND_HITS" >&2
    exit 1
fi
echo "    plugin/src/main names no backend"

# tools/ holds the JUnit launcher and PublishNodes, which the milestone-2 acceptance check drives. Built
# even with --no-tests, because tools/supervisor-sees-me.sh needs it and does not run a single test.
echo "==> tools"
compile "tools" tools "$OUT/tools" "$OUT/plugin/main:$OPENSEARCH_CP:$STORE_CP:$JUNIT_CP"

if [[ $RUN_TESTS -eq 0 ]]; then
    echo "==> done (--no-tests)"
    exit 0
fi

# The shared conformance suite, from the repository that owns NODE_CONTRACT.md. Compiled from source for
# the same reason store-api is: two implementations of one document format that drift apart is the whole
# thing this suite exists to prevent, and consuming a stale jar would let them.
echo "==> node contract fixtures (from source: $SUPERVISOR)"
CONTRACT_CP=""
if [[ -d "$SUPERVISOR/fake-node/src/testFixtures/java" ]]; then
    compile "supervisor-model" "$SUPERVISOR/supervisor-model/src/main/java" "$OUT/supervisor-model/main" "$JACKSON_CP"
    compile "fake-node main" "$SUPERVISOR/fake-node/src/main/java" "$OUT/fake-node/main" \
        "$OUT/supervisor-model/main:$STORE_CP:$JACKSON_CP"
    compile "fake-node testFixtures" "$SUPERVISOR/fake-node/src/testFixtures/java" "$OUT/fake-node/testFixtures" \
        "$OUT/fake-node/main:$OUT/supervisor-model/main:$STORE_FIXTURES_CP:$JACKSON_CP:$JUNIT_CP"
    CONTRACT_CP="$OUT/fake-node/testFixtures:$OUT/fake-node/main:$OUT/supervisor-model/main:$JACKSON_CP"
else
    echo "    SKIPPED: no fixtures at $SUPERVISOR/fake-node/src/testFixtures/java"
    echo "    The conformance tests will not be compiled or run, which is a hole rather than a pass."
fi

echo "==> tests"
compile "plugin tests" plugin/src/test/java "$OUT/plugin/test" \
    "$OUT/plugin/main:$OPENSEARCH_CP:$STORE_FIXTURES_CP:$CONTRACT_CP:$JUNIT_CP"

echo "==> running tests"
TEST_CP=$(join_cp "$OUT/tools" "$OUT/plugin/main" "$OUT/plugin/test" \
    "$OPENSEARCH_RUNTIME_CP" "$STORE_FIXTURES_CP" ${CONTRACT_CP:+"$CONTRACT_CP"} "$JUNIT_CP")

# OpenSearchTestCase runs under RandomizedRunner, which wants to know where it may write and expects the
# same properties Gradle's test task sets. tests.security.manager is off because OpenSearch 3.x no longer
# installs one; the temp directory is where LuceneTestCase's createTempDir() puts things and then reaps them.
RUNNER_FLAGS=(
    # LuceneTestCase refuses to run with assertions off, and it is right to: half of what the OpenSearch
    # codebase checks at runtime is an assert. Gradle's test task passes -ea for the same reason.
    "-ea"
    "-Dtests.security.manager=false"
    # sandbox_init() is denied in here, so the filter logs a warning and carries on. Off explicitly, because a
    # warning that appears on every run trains people to ignore warnings.
    "-Dtests.system_call_filter=false"
    "-Djava.io.tmpdir=$OUT/tmp"
    "-Dtests.seed=DEADBEEF"
)
mkdir -p "$OUT/tmp"

if [[ ${#TEST_PATTERNS[@]} -gt 0 ]]; then
    "$JAVA" "${RUNNER_FLAGS[@]}" -cp "$TEST_CP" tools.JUnit4Runner "$OUT/plugin/test" -- "${TEST_PATTERNS[@]}"
else
    "$JAVA" "${RUNNER_FLAGS[@]}" -cp "$TEST_CP" tools.JUnit4Runner "$OUT/plugin/test"
fi
