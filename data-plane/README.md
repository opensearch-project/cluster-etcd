# OpenSearch data plane

The third plane: an OpenSearch plugin that makes a running node act on desired state read from a shared
key/value store, with **no cluster manager anywhere in the deployment**.

The joke that explains the design better than a paragraph would: the plugin is the server's *voice in its
head*, pretending to be the cluster manager. OpenSearch already has a well-worn path for "the cluster manager
published new state, apply it" — the plugin's job is to walk that path with state it read from a store rather
than state it received from an elected node.

## Status

**Milestones 1 and 2 are done: this plugin publishes, and does not yet apply.** A node with it installed
opens the store, writes `nodes/registration/<id>`, `nodes/heartbeat/<id>` and `nodes/shards/<id>`, and keeps
them current. It reads nothing and fabricates no `ClusterState`, so there is nothing it can get wrong that
would move data — which is the whole reason for doing it in this order.

The specification is still the point of this directory: `REQUIREMENTS.md` for what the thing must do,
`MILESTONES.md` for the order to build it in, and `BORROWING_FROM_CLUSTER_ETCD.md` for a survey of the
implementation next door saying which parts to lift, which to adapt and which are now useless.

### Building it

```
./gradlew build                    # the build; needs ../OpenSearch installed to mavenLocal first
tools/jbuild.sh                    # compile and test with javac, for environments where Gradle cannot run
tools/supervisor-sees-me.sh        # milestone 2's acceptance check, against a real supervisor
./gradlew :plugin:schemes          # which store backends this build can see
```

`tools/supervisor-sees-me.sh` is the one worth running first. It publishes node documents into a scratch
store with the plugin's own publisher, runs a **real supervisor** against them in read-only mode, and
prints what it made of them:

```
  nodes: 2
  placeable_nodes: 2
  findings_by_kind: {"UNASSIGNED_PRIMARY": 2}
  unreadable_documents: 0
```

Two live, placeable nodes, and an index the supervisor now wants to place on them. No OpenSearch node and
no container, because the only part of the plugin involved is the part that talks to the store — which is
exactly what this milestone is.

The plugin targets **OpenSearch 3.9**, the first version carrying the clusterless server path it steers —
`LocalClusterService`, `LocalDiscovery`, `LocalShardStateAction`. Build the sibling checkout once
(`cd ../OpenSearch && ./gradlew publishToMavenLocal`) before building here.

`tools/jbuild.sh` exists because Gradle cannot run in the sandbox this was written in: it fails creating
its file-lock service, since binding a socket is denied. It compiles every module against the OpenSearch
jars in `../OpenSearch` and runs the tests through a hand-rolled JUnit launcher. **It is not the build** —
it skips plugin packaging, forbidden-APIs and the license checks — so `./gradlew build` is currently
unverified and should be run by someone who can.

### Configuration

| setting | |
|---|---|
| `clusterless.store` | **required.** A store URI, e.g. `file:///var/lib/clusterless` or `etcd://host:2379/opensearch`. The scheme selects a backend jar on the classpath; nothing in this repository names one. |
| `clusterless.node.pool` | the pool this node was deployed into. Optional — `pools/nodes/<id>` overrides it, and a node in neither is `NODE_UNPOOLED`, which is a visible finding rather than a default. |
| `clusterless.node.roles` | `data`, `coordinator`, or both. Defaults to a derivation from OpenSearch's own `node.roles`. |
| `clusterless.heartbeat.interval` | default 5s. Published in the registration, and the supervisor derives this node's staleness threshold from it, so it must be the interval the node really writes at. |
| `clusterless.poll.interval` | default: the heartbeat interval. Separate because the costs are different — one is a write by every node in the fleet, the other a read of one small key. |

`GET /_clusterless/status` reports what this node last read, applied and wrote.


## The three planes, and why this one is last

| plane | repository | lifetime of its work | owns |
|---|---|---|---|
| control | `../opensearch-control-plane` | milliseconds — a CRUD request | what the user asked for |
| management | `../opensearch-supervisor` | seconds — a reconciliation pass | where shards should be |
| **data** | **here** | continuous | shards, queries, and the truth about both |

They communicate **only** through the store. No plane calls another. That is the property that lets any of
them be restarted, scaled or replaced without the others noticing, and it is worth defending against every
convenience that would break it.

This one is specified last on purpose. The seam it implements —
[`../opensearch-supervisor/NODE_CONTRACT.md`](../opensearch-supervisor/NODE_CONTRACT.md) — was written from the
document first and then implemented twice: once by the supervisor's observer, and once by `fake-node`, which
was deliberately written *from the document rather than from the supervisor* so that the contract had a second
independent reader before it had a real one. Several gaps were found that way. **You are the third reader, and
the first with a real OpenSearch process behind you** — so where the document is wrong or silent, that is a
finding to report back, not something to paper over.

## Read these, in this order

1. **[`REQUIREMENTS.md`](REQUIREMENTS.md)** — this document's own requirements, numbered `D*`.
2. **[`MILESTONES.md`](MILESTONES.md)** — an order of work. Start with its fifteen-minute orientation: run the
   supervisor against a directory and read what it writes.
3. **[`../opensearch-supervisor/NODE_CONTRACT.md`](../opensearch-supervisor/NODE_CONTRACT.md)** — *the*
   authoritative specification of every document this plugin writes and reads. It is long and every paragraph
   in it is load-bearing. If this README and that document disagree, that document wins.
4. **[`../opensearch-supervisor/KEY_LAYOUT.md`](../opensearch-supervisor/KEY_LAYOUT.md)** — the key space,
   including the rules about uuids versus names that a reconciler keyed on the wrong one gets subtly wrong.
5. **[`BORROWING_FROM_CLUSTER_ETCD.md`](BORROWING_FROM_CLUSTER_ETCD.md)** — what is reusable from
   `../cluster-etcd/plugin` and what is not.
6. **[`../opensearch-kv-store/README.md`](../opensearch-kv-store/README.md)** — the store SPI.

`../opensearch-supervisor/fake-node/` is a working implementation of the node side of the contract in about 300
lines. It holds no data and serves nothing, so it is not a model for the real thing — but it is an executable
answer to "what exactly is this document asking me to write", and it is what the supervisor's own tests drive.

## The shape of it

```
                        ┌──────────────────────────────┐
                        │      key/value store         │
                        └──────────────────────────────┘
    writes: nodes/registration/<id>  │  reads: nodes/assignment/<id>
            nodes/heartbeat/<id>     │         tenant/<t>/indices/<name>
            nodes/shards/<id>        │         tenant/<t>/status/indices/<uuid>
                                     │         tenant/<t>/status/nodes/<id>
                        ┌────────────┴─────────────────┐
                        │   this plugin                │
                        │   ┌──────────────────────┐   │
                        │   │ fabricate ClusterState│  │
                        │   └──────────┬───────────┘   │
                        │  ClusterApplierService       │
                        │      .updateClusterState()   │
                        └────────────┬─────────────────┘
                        ┌────────────┴─────────────────┐
                        │  the ordinary OpenSearch node │
                        │  recovery, search, indexing   │
                        └──────────────────────────────┘
```

Three writes, four reads, and one call into the server. Everything else is detail.

## Two things that will be tempting and are wrong

**Reacting to a watch event instead of to observed state.** The store SPI exposes an optional `watch`, and it
is genuinely useful — it lets a node apply an assignment in milliseconds instead of on its next poll. But a
watch is a *latency optimisation over polling*, never the thing that makes convergence correct. Every backend
can drop, coalesce or replay notifications. `D2.1` is the requirement and it is the one to hold hardest.

**Trusting the supervisor about anything except desired state.** The supervisor writes assignments. It does
not know what this node holds — it finds out by reading what this node published. So the inventory has to be
the truth even when the truth is embarrassing, and especially when it disagrees with the assignment. A plugin
that reported what it was told to hold rather than what it holds would make the whole system blind in a way
nothing downstream could detect.
