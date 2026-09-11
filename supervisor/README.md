# OpenSearch supervisor

The management plane: the service that makes reality match the control plane's intent. It reads desired
state from a shared key/value store, reads actual state from data plane heartbeats, and issues instructions
until the two agree.

## Status

**All seven milestones done, plus allocation and balancing, and it runs.** `DESIGN.md` §11 has the plan it
followed.

What exists: observation of the whole node contract, revision-silence liveness, pool leases and the balancer
that shares pools between supervisors, the differ, the planner, the applier, a task queue, the observability
projections, eight allocation deciders, five rebalance deciders with a size-aware load model, and a runnable
process.

What does not, in rough order of how much it matters:

- **No primary movement or failover.** Moving a primary needs a per-primary generation so a superseded primary
  can be fenced, and that is work in OpenSearch itself. `PrimaryMoveDecider` refuses, `SurplusReaper` never
  drops a primary, and `DESIGN.md` §13 open question 1 carries the detail. A pool whose imbalance is all in
  primaries cannot be relieved.
- **Never run against a real store.** Every test uses the in-memory store, and the end-to-end tests use the
  bundled filesystem one. `scanPrefix` is not snapshot-consistent on every backend and `DESIGN.md` §12 reasons
  about that rather than having observed it.
- **No snapshot metadata projection** (R5.3), which needs repository access.
- **No cross-pool migration**, and no per-shard replica autoscaling — though §15 records the seam for the
  second so that nothing forecloses it.

## Running it

```
./gradlew :supervisor-app:installDist
./supervisor-app/build/install/supervisor-app/bin/supervisor-app --help
```

It is **read-only unless you pass `--write`**. That is deliberate: this service creates and removes shard
copies, so starting in the wrong mode because of a forgotten flag should cost an afternoon rather than a
cluster's data. Read-only observes, diffs and projects everything, so you can read what it *would* do out of
the store before letting it act.

A bounded smoke test against a directory, needing no backend at all:

```
./supervisor-app/build/install/supervisor-app/bin/supervisor-app \
    --store=file:///tmp/supervisor-demo --max-passes=5 --interval=200ms
```

That writes `supervisor/status/pools/<pool>/convergence` and a diff per tenant, which is where the answers
live. Against a real backend, name its scheme and put that backend's jar on the runtime classpath:

```
./gradlew :supervisor-app:installDist -PstoreJars=org.opensearch.controlplane:store-etcd:0.1.0-SNAPSHOT
./supervisor-app/build/install/supervisor-app/bin/supervisor-app \
    --store=etcd://localhost:2379/opensearch --max-passes=5
```

Nothing in this repository names etcd or ZooKeeper at compile time — `KeyValueStores` finds backends through
`ServiceLoader` — so `--help` lists whichever ones it can actually find, which is what makes a missing jar
distinguishable from a typo.

Every option can also come from the environment (`--lease-ttl` is `SUPERVISOR_LEASE_TTL`), with the command
line winning.

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

## Integration tests

`supervisor-integ` runs against a real etcd. It is a separate module so that jetcd, gRPC and a Docker client
stay off every other module's classpath.

```
./gradlew :supervisor-integ:test                              # starts etcd in Docker
./gradlew :supervisor-integ:test -Dsupervisor.etcd=localhost:2379   # use one you already have
```

Without Docker they skip, with a message on stderr saying why — and distinguishing the two failures that look
identical but need opposite responses: DOCKER_HOST not discovered, versus discovered but nothing listening on
the socket.

The root build sets `DOCKER_HOST` from `docker context inspect` when it is not already set. Testcontainers only
finds the daemon unaided when the socket is at `/var/run/docker.sock`, and colima, Rancher Desktop and Apple's
`container` tool all put it somewhere else — so the symptom is the integration tests skipping on a machine where
`docker` works perfectly, which is a green build that tested nothing. Running them outside Gradle needs the same
thing by hand:

```
export DOCKER_HOST=$(docker context inspect --format '{{.Endpoints.docker.Host}}')
``` **They take minutes when they do run**: lease
TTLs and revision-silence liveness are measured in wall-clock seconds and there is no clock to advance, which
is precisely what they exist to exercise — every other test in the repository uses a clock it can move by hand.

They are deliberately not a second copy of the unit suite. Three things only a real store and a real clock can
answer:

- **`EtcdStoreAssumptionsIT`** — the claims `DESIGN.md` makes about store semantics, as executable assertions:
  that an identical-bytes rewrite still changes the revision (liveness depends on it, and the shared contract
  suite does not cover it), that a stale compare-and-set is rejected, that prefixes do not overlap, that a
  full-size inventory survives the backend. One honest caveat, also stated in the file: etcd answers a prefix
  scan with one atomic range request, so the torn-read assertions from §12 pass here almost by construction.
  They are written so that a backend which walks the subtree — ZooKeeper — would fail them loudly. Running
  this suite against ZooKeeper is the natural follow-on.
- **`EtcdLeaseIT`** — several supervisors on real threads against real compare-and-set, asserting that no pool
  is ever held by two at once, that a clean shutdown hands over without waiting out a TTL, and that an abrupt
  death is recovered from observed silence with the epoch advanced. Every previous fleet test took turns from a
  single thread, which is the one arrangement in which a lease cannot fail.
- **`EtcdConvergenceIT`** — an index placed across real nodes reporting green, then staying settled; and a node
  going silent against a clock nobody controls, with its copies rebuilt elsewhere.

## Testing

Risk goes where no network is needed — `DESIGN.md` §10 has the table. The diff and the plan are pure
functions of an observation, so the interesting half is tested with hand-built inputs and no store at all.
Store-touching code runs against `InMemoryKeyValueStore`, taken from store-api's test fixtures rather than
reimplemented: two guesses at the same revision semantics is how the planes would silently disagree.

Anything needing a real etcd, ZooKeeper or OpenSearch **skips** rather than fails, and says why on stderr.
