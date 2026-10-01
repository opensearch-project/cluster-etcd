# opensearch-kv-store

The persistence seam shared by the OpenSearch control plane and supervisor: an ordered key/value store of
opaque byte payloads, with per-key revisions and conditional writes.

| module | what |
|---|---|
| `store-api` | the `KeyValueStore` interface, the `KeyValueStoreProvider` SPI, the filesystem implementation, and — as test fixtures — `InMemoryKeyValueStore` plus `KeyValueStoreContractTests` |
| `store-zookeeper` | ZooKeeper backend, verified against 3.9 |
| `store-etcd` | etcd backend, verified against 3.7 |

`store-api` has **no dependencies at all**, deliberately: a backend depends on it and on its own client
library, and anything added here would be inherited by every backend and every service.

## Why this is its own build

Both planes read and write the same keys. Two implementations of the same revision semantics is how they
would silently disagree, so there is one implementation and both depend on it.

## Using it

Consumers include it as a Gradle composite build, so there is no publish step while the services are being
developed together:

```kotlin
// settings.gradle.kts
includeBuild("../kv-store")

// build.gradle.kts
implementation("org.opensearch.controlplane:store-api:0.1.0-SNAPSHOT")
testImplementation(testFixtures("org.opensearch.controlplane:store-api:0.1.0-SNAPSHOT"))
```

The cost is that a consumer's build depends on a path outside itself, which CI satisfies by checking out
both. A submodule or a published artifact removes that later without undoing this layout.

## Watches

`KeyValueStore.watch(prefix, onChange)` is optional per backend. Consumers should not branch on that — call
`Watches.watchOrPoll(...)`, which uses a native watch where one exists and polls where it does not, so the
same consumer code works against every backend including the filesystem one.

**A notification carries no payload and no guarantees.** It means "something under your prefix may have
changed; re-read it", and may be coalesced, spurious, or missed. This is the deliberate lowest common
denominator: etcd could support far more, ZooKeeper less, and polling least of all, so promising the union
would mean lying about two of the three. It also makes the safe consumer design the only one available —
an API handing over changed keys invites applying just those, which breaks silently at the first missed
notification.

| backend | how |
|---|---|
| `store-zookeeper` | native, `AddWatchMode.PERSISTENT_RECURSIVE` — one registration covers a subtree, needs no re-arming, and does not require the prefix to exist yet |
| `store-etcd` | native range watch, from now rather than a past revision |
| `store-api` filesystem | polling |

## What every backend guarantees

The `KeyValueStore` Javadoc is the authority; in short:

- **Revisions** are never reused for a key, even across delete and recreate or a rewrite of identical
  bytes. They strictly increase per key and stay at most 2^53 − 1. **Nothing is promised across keys.**
- **Conditional writes are linearizable per key.** A store instance reads its own writes. Reads through
  different instances may lag by seconds. Scans are not snapshots. Acknowledged writes survive failover.
- **Keys up to 512 UTF-8 bytes and values up to 960 KiB**, enforced identically by every backend with
  `LimitExceededException`, so a consumer that works on one backend works on all of them.

[`BACKENDS.md`](BACKENDS.md) is where these came from: sketches of DynamoDB, relational, OpenSearch and S3
backends, and what each one needs from the interface.

## Writing a backend

Implement `KeyValueStore` and `KeyValueStoreProvider`, declare the provider in
`META-INF/services/org.opensearch.controlplane.store.KeyValueStoreProvider`, and extend
`KeyValueStoreContractTests`. Three of its assertions constrain the design more than they look:

- **`emptyValuesRoundTrip`** — an empty value must stay distinguishable from an absent key, because marker
  keys rely on it. A backend may not use "has no data" to mean "is not a key".
- **`dotDotIsAnOrdinaryKeySegment`** — `..` is an ordinary key segment and must never resolve, and
  `scanPrefix` is specified over key prefixes rather than path boundaries, so a non-segment-aligned prefix
  such as `tenant/acme/x` has to work too.
- **`aKeyAndValueAtTheLimitsRoundTrip`** — the longest key and the largest value together, after the
  backend's own escaping and framing. A backend with smaller native limits works around them; it does not
  lower the limits. The filesystem store hashes long filenames, for example.

## Running the integration tests

Each backend's contract tests need a real server, and find one in this order:

1. **`-Dcontrolplane.<backend>=host:port`** — a server you are already running. Wins when set, because
   starting a container costs seconds and pointing the suite at a specific build is sometimes the point.
2. **A container**, if Docker is available. This is what CI uses: **no configuration, just Docker.**
3. Otherwise the tests **skip**, which is not a failure — this repository is also worked on in an
   environment with no Docker and no outbound sockets, where `ZooKeeperKeysTests` still runs and still
   carries most of the risk.

```bash
../gradlew test                                          # containers, if Docker is up
../gradlew :store-etcd:test -Dcontrolplane.etcd=localhost:2379   # a server you already have
../gradlew :store-zookeeper:test -Dcontrolplane.zookeeper.image=zookeeper:3.8   # a different version
```

One container per JVM, started lazily and left for Ryuk to reap. Every test isolates itself in a fresh
root znode or key prefix, so sharing one is safe and starting thirty is not worth the minutes.

### If they skip on a machine where Docker works

Testcontainers finds the daemon by itself only when the socket sits at the conventional path. Colima,
Rancher Desktop, and a Docker Desktop with the default socket turned off all put
it elsewhere, and the symptom is these tests skipping while `docker` works perfectly.

The build handles that: each `test` task asks the Docker CLI where its current context points
(`docker context inspect`) and passes the answer to the test JVM as `DOCKER_HOST`, along with
`TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE` so Ryuk can reach the daemon from inside its container. Setting
`DOCKER_HOST` yourself takes precedence.

If a run still skips, it prints the reason — the message Testcontainers gave, the environment it saw, and a
stack trace. Two escapes worth knowing:

```bash
# Ryuk cannot always talk to a non-Docker-Desktop daemon. Containers then need reaping by hand.
TESTCONTAINERS_RYUK_DISABLED=true ../gradlew test

# Or skip containers entirely and use a server you are already running.
../gradlew :store-etcd:test -Dcontrolplane.etcd=localhost:2379
```
