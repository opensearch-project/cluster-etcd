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
includeBuild("../opensearch-kv-store")

// build.gradle.kts
implementation("org.opensearch.controlplane:store-api:0.1.0-SNAPSHOT")
testImplementation(testFixtures("org.opensearch.controlplane:store-api:0.1.0-SNAPSHOT"))
```

The cost is that a consumer's build depends on a path outside itself, which CI satisfies by checking out
both. A submodule or a published artifact removes that later without undoing this layout.

## Writing a backend

Implement `KeyValueStore` and `KeyValueStoreProvider`, declare the provider in
`META-INF/services/org.opensearch.controlplane.store.KeyValueStoreProvider`, and extend
`KeyValueStoreContractTests`. Two of its assertions constrain the design more than they look:

- **`emptyValuesRoundTrip`** — an empty value must stay distinguishable from an absent key, because marker
  keys rely on it. A backend may not use "has no data" to mean "is not a key".
- **`dotDotIsAnOrdinaryKeySegment`** — `..` is an ordinary key segment and must never resolve, and
  `scanPrefix` is specified over key prefixes rather than path boundaries, so a non-segment-aligned prefix
  such as `tenant/acme/x` has to work too.

Tests that need a real server go behind `@EnabledIfSystemProperty` and skip without one:

```bash
./gradlew :store-zookeeper:test -Dcontrolplane.zookeeper=localhost:2181
./gradlew :store-etcd:test      -Dcontrolplane.etcd=localhost:2379
```
