# Clusterless OpenSearch

OpenSearch without a cluster manager. Nodes never discover each other or elect anything: everything they share,
they share through a key/value store, and three services divide the work a cluster manager used to do.

| directory | what it is |
|---|---|
| [`control-plane/`](control-plane/README.md) | Serves the cluster manager APIs — index CRUD, mappings, settings, aliases, pipelines — and records what is wanted in the store. Talks to no node. |
| [`supervisor/`](supervisor/README.md) | Compares what is wanted with what nodes report, and assigns shard copies to nodes. |
| [`data-plane/`](data-plane/README.md) | An OpenSearch plugin that makes a node act on its assignment, with a cluster state holding only what it needs. |
| [`kv-store/`](kv-store/README.md) | The store API the other three share, with file, ZooKeeper and etcd backends. |

[`DEMO.md`](DEMO.md) walks through all three services on one machine.

This is `v2`, a rewrite. The original cluster-etcd plugin and controller are on the `main` branch.

## Building

```bash
./gradlew build
```

builds and tests every component. The root is a composite of the four builds, and each still builds on its own
from its directory with `../gradlew`. A single module is addressed by directory name:
`./gradlew :supervisor:supervisor-core:test`.

It needs Java 21, and the data plane needs OpenSearch 3.9 in mavenLocal:
`cd /path/to/OpenSearch && ./gradlew publishToMavenLocal`.

## License

Apache 2.0. See [`control-plane/LICENSE.txt`](control-plane/LICENSE.txt).
