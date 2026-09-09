# OpenSearch supervisor

The management plane: the service that makes reality match the control plane's intent. It reads desired
state from a shared key/value store, reads actual state from data plane heartbeats, and issues instructions
until the two agree.

## Status

**Milestone 1 of 7: the skeleton.** The module graph, the composite build, and a sandbox build fallback.
There is no reconciliation loop yet, and nothing here writes an assignment. `DESIGN.md` §11 has the plan;
the short version is that the service will observe and report for two milestones before it acts on
anything.

What exists: identifiers, the key layout as executable code, and tenant discovery.

## Where to read

| document | what |
|---|---|
| [`REQUIREMENTS.md`](REQUIREMENTS.md) | what this service must do, and why — written before any code |
| [`DESIGN.md`](DESIGN.md) | how it works: the pool partition model, the loop, leases, testing, milestones |
| [`KEY_LAYOUT.md`](KEY_LAYOUT.md) | what the **control plane** writes, and what this service may read |
| [`NODE_CONTRACT.md`](NODE_CONTRACT.md) | the **data plane** seam: heartbeat, registration, inventory, assignment |
| [`SUPERVISOR_KEYS.md`](SUPERVISOR_KEYS.md) | what this service writes: projections, pools, leases, observability |
| [`WORKING_NOTES.md`](WORKING_NOTES.md) | what this development environment blocks, and how to work in it |

The three key documents are contracts with two other services. A contract in prose drifts, so
`supervisor-model`'s `Keys` class is those documents made executable and `KeysTests` pins them: a layout
change should be one edit and a failing test, not a search for string concatenation.

## Modules

```
supervisor-model/    records, identifiers, key layout, JSON codecs. No dependencies. No store.
supervisor-core/     observe, diff, plan, apply, leases, balancer, loop. The only module that sees the store.
supervisor-app/      main, configuration, wiring.
fake-node/           a data plane test double. Milestone 2.
```

`supervisor-model` must never depend on `store-api`. `DESIGN.md` §1 claims the diff is a pure function of
an observation, and a module that cannot see a `KeyValueStore` cannot quietly read a key it forgot to
observe. The module graph is what makes that claim checkable at build time rather than at review time.

## Building

```bash
./gradlew build
```

Verified green on a Java 21 toolchain — Temurin 21.0.12.1. Gradle's auto-detection usually finds it without
configuration; if yours cannot, point it at one with
`-Dorg.gradle.java.installations.paths=...`.

The persistence seam is shared with the control plane and lives in its own build,
[`../opensearch-kv-store`](../opensearch-kv-store), consumed here as a Gradle **composite build**. So this
build depends on a path outside itself, and **CI has to check out both repositories.** A submodule or a
published artifact removes that later without changing the module layout.

Substitution matches on group and name, so the coordinate must stay exactly
`org.opensearch.controlplane:store-api:0.1.0-SNAPSHOT` — change either and it resolves against a repository
instead of the local project, and fails.

### Building where Gradle cannot run

Gradle fails in the development sandbox this was written in: it cannot create its file-lock service,
because binding a socket is denied.

```
> Could not create service of type FileLockContentionHandler
   > java.net.SocketException: Operation not permitted
```

So there is a fallback, which compiles with `javac` against the jars already in `~/.gradle/caches` and
`~/.m2`, and runs the same tests through a hand-rolled JUnit Platform launcher:

```bash
tools/jbuild.sh                 # compile everything, run every test
tools/jbuild.sh --no-tests      # compile only
tools/jbuild.sh KeysTests       # run tests whose class name matches
```

It uses the same Java 21 toolchain Gradle does, found via `/usr/libexec/java_home -v 21` or `JAVA21_HOME`,
rather than `--release 21` on whatever `javac` is first on the path. `--release` gets the language level
and the API surface right but still runs a newer compiler and a newer JVM, and the point of the script is
to behave like the Gradle build rather than approximately like it. It falls back to `--release` with a
warning if no 21 toolchain is found.

**`./gradlew build` is the build; `tools/jbuild.sh` is a way to keep working when it cannot run.** Anything
the fallback has to special-case is a difference between the two, and the difference is the bug.

The fallback compiles `store-api` from source rather than using
`../opensearch-kv-store/store-api/build/libs`, so a change over there is picked up immediately. Set
`KV_STORE=/path/to/opensearch-kv-store` if it is not beside this repository.

## Testing

Risk goes where no network is needed — `DESIGN.md` §10 has the table. The diff and the plan are pure
functions of an observation, so the interesting half is tested with hand-built inputs and no store at all.
Store-touching code runs against `InMemoryKeyValueStore`, taken from store-api's test fixtures rather than
reimplemented: two guesses at the same revision semantics is how the planes would silently disagree.

Anything needing a real etcd, ZooKeeper or OpenSearch **skips** rather than fails, and says why on stderr.
