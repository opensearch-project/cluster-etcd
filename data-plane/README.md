# OpenSearch data plane

The third plane: an OpenSearch plugin that makes a running node act on desired state read from a shared
key/value store, with **no cluster manager anywhere in the deployment**.

The joke that explains the design better than a paragraph would: the plugin is the server's *voice in its
head*, pretending to be the cluster manager. OpenSearch already has a well-worn path for "the cluster manager
published new state, apply it" — the plugin's job is to walk that path with state it read from a store rather
than state it received from an elected node.

## Status

**Milestones 1 to 4 are done, and the whole loop has run against a real node.** A node with the plugin installed
opens the store, writes `nodes/registration/<id>`, `nodes/heartbeat/<id>` and `nodes/shards/<id>`, reads
`nodes/assignment/<id>` and the `tenant/<t>/indices/<name>` documents it names, fabricates a `ClusterState` and —
with `clusterless.apply` — hands it to the server.

What that has been observed doing, with a control plane, a supervisor and a data node sharing one store: an index
created through the control plane is planned by the supervisor, applied here, and recovers to `STARTED` with a
real `size_bytes` in the inventory. It can be searched. It can be **written to**:

```
$ curl -XPOST .../default.my-index.c526cf4262/_doc/1 -d '{"title":"foo"}'
{"result":"created","_shards":{"total":1,"successful":1,"failed":0},"_seq_no":0,"_primary_term":1}
```

Closing the index releases the shard without deleting its files, and reopening it recovers with
`EXISTING_STORE` — which was the one part of the fabrication with no precedent and no test that could prove it.

**Two things in that write response are worth reading twice.** `"total":1` where a cluster with one replica would
say 2: the accounting comes from the fabricated state, and a data node's state contains only its own copies, so
`_shards` reports what this node knows rather than what the fleet has. And `"_primary_term":1`, which is D8.2 made
visible — every write this system makes is stamped term 1, because nothing in the contract carries a term to
fabricate from, so the fencing a term normally provides is absent and the supervisor never failing over a primary
is what stands in for it.

**What is not proven.** The index reaches *yellow*, not green: it was created with one replica, document
replication is the default, and a docrep replica needs peer resolution — milestone 9. Green needs either zero
replicas, or a segment-replication index across two nodes (milestone 7). And the sandbox most of this was written
in cannot start a node at all, so anything not on that list has been reasoned about rather than seen.

Read-only is the default, mirroring the supervisor's, and for the same reason: this is the component that can
move data, so it explains itself for a while first, somewhere somebody can check the explanation. In that mode
the node reports `outcome: failed` on its heartbeat rather than claiming to have applied anything — the
supervisor should see a node that is not doing what it was told, because it is not.

The specification is still the point of this directory: `REQUIREMENTS.md` for what the thing must do,
`MILESTONES.md` for the order to build it in, and `BORROWING_FROM_CLUSTER_ETCD.md` for a survey of the
implementation next door saying which parts to lift, which to adapt and which are now useless.

### Building it

```
./gradlew build                    # the build; needs ../OpenSearch installed to mavenLocal first
tools/jbuild.sh                    # compile and test with javac, for environments where Gradle cannot run
tools/supervisor-sees-me.sh        # milestone 2's acceptance check: the supervisor sees this node
tools/supervisor-assigns-me.sh     # milestone 3's: it assigns a shard, and the node explains what it would do
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

Two live, placeable nodes, and an index the supervisor now wants to place on them.

`tools/supervisor-assigns-me.sh` goes a step further: it runs the supervisor in `--write` mode so that it
really assigns the shards, then runs one read-only pass and prints what would have been applied —

```
intended cluster state: version 1, from assignment version 1, 1 index(es), 2 copy(ies)
  index acme.my-index.7d84389e80
    identity: tenant=acme name=my-index uuid=uzKxLWZu4nPSrTknEcDpoA config_revision=1
    metadata: version=1 settings_version=1 mapping_version=1 shards=2 replicas=0
    settings: {index.number_of_replicas=0, index.number_of_shards=2, index.replication.type=SEGMENT, ...}
    mappings: {"_doc":{"properties":{"host":{"type":"keyword"}}}}
    shard 0: primary_term=1 in_sync=[A4N5GG...] [PRIMARY INITIALIZING on dp-node-0 recovery=EMPTY_STORE]
```

Neither needs an OpenSearch node or a container. Everything from the store to the fabricated `ClusterState` is
ordinary Java; only the last step — handing that state to `ClusterApplierService` — needs a running node, and
that is the next milestone.

### Running a real node

```
./gradlew run                              # a node with the plugin installed, applying what it is assigned
./gradlew run -Pclusterless.apply=false    # the same, read-only
./gradlew run -Pclusterless.nodes=2        # two of them, sharing one store
./gradlew run -Pclusterless.remote_store=false   # no remote store, so search replicas are declined
./gradlew run -Pclusterless.segments_only=true   # segments in the repository, translog on local disk
./gradlew run -Pclusterless.coordinating=true    # every node also coordinates, which is how to test routing today
./gradlew runCoordinator                   # a dedicated coordinator -- see the caveat below
```

**To exercise coordinator routing on one machine, use `-Pclusterless.coordinating=true`.** Each node then holds
copies *and* routes to every other node's, so a search against any of them returns the whole index. That is a real
configuration rather than a workaround — OpenSearch's own default is that every node can coordinate, and an
assignment's tenant roles are a set precisely so a node can be told it does both.

`runCoordinator` is the dedicated-coordinator shape, which is where coordinators belong in production. It cannot
currently run *alongside* `./gradlew run`: `RunTask.beforeStart()` hardcodes 9200 and 9300 and overwrites whatever
the build file sets, so two invocations collide. `WORKING_NOTES.md` has the patch that would fix it.

`runCoordinator` is a **separate cluster**, not an extra node, and that is the natural shape rather than a
workaround: clusterless nodes never discover each other, the transport does not compare cluster names, and
everything they share they share through the store — so two testClusters clusters are peers in the only sense that
matters. It also matches where coordinators belong, which is their own capacity. Run it in a second terminal
alongside `./gradlew run`.

The run cluster also configures a remote store by default — an `fs` repository under
`plugin/build/clusterless-remote-store`, shared by every node in the run, declared through
`node.attr.remote_store.*`. That is what makes a search replica possible at all: a replica that is not
remote-backed has nowhere to read from, because the alternative source resolves its primary through a routing
table that a clusterless node's state does not contain. To see a search replica start, ask the control plane for
an index with `index.replication.type: SEGMENT` and one replica, and run two nodes.

The run cluster points at `plugin/build/clusterless-store`, and a supervisor pointed at the same directory is
the other half of the loop:

```
cd ../opensearch-supervisor && ./gradlew :supervisor-app:installDist
SUP=./supervisor-app/build/install/supervisor-app/bin/supervisor-app
STORE=file://<repo>/plugin/build/clusterless-store

$SUP --store=$STORE --define-pool=shared-a     # once: nothing happens at all until a pool exists
$SUP --store=$STORE --write --interval=1s      # then reconcile
```

**`--define-pool` is not optional the first time.** A pool definition is operator-owned — only an operator knows
which node attribute names a thing that fails independently — so nothing in any of the three planes writes one,
and until one exists there are no pools and therefore no reconciliation: the control plane accepts an index,
nodes register and heartbeat, and nothing is ever assigned. The only thing that says why is `unpooled_tenants` in
`supervisor/status/housekeeping`. The `run` cluster puts its nodes in `shared-a`, which is why that is the name
above.

Note that `PluginBuildPlugin` registers `run` but attaches no cluster to it — only `RestIntegTestTask` gets one
automatically — so `plugin/build.gradle` creates and uses one. It also has to set `clusterless.store`, since
there is no default and a node without one refuses to start.

The plugin targets **OpenSearch 3.9**, the first version carrying the clusterless server path it steers —
`LocalClusterService`, `LocalDiscovery`, `LocalShardStateAction`. Build the sibling checkout once
(`cd ../OpenSearch && ./gradlew publishToMavenLocal`) before building here.

`tools/jbuild.sh` exists because Gradle cannot run in the sandbox much of this was written in: it fails
creating its file-lock service, since binding a socket is denied. It compiles every module against the
OpenSearch jars in `../OpenSearch` and runs the tests with JUnit 4 under `RandomizedRunner`, which is the
framework the real build uses — matching it is the whole point, since a launcher that discovered tests Gradle
would not, or the reverse, reports a green build for a suite nobody runs.

**`./gradlew build` passes**, and the two agree test for test. It remains the authority, because
it also runs plugin packaging, forbidden-APIs, `testingConventions` and the license checks, none of which
`jbuild.sh` reproduces. Three of those four have already caught something the script could not.

### Configuration

| setting | |
|---|---|
| `clusterless.store` | **required.** A store URI, e.g. `file:///var/lib/clusterless` or `etcd://host:2379/opensearch`. The scheme selects a backend jar on the classpath; nothing in this repository names one. |
| `clusterless.node.pool` | the pool this node was deployed into. Optional — `pools/nodes/<id>` overrides it, and a node in neither is `NODE_UNPOOLED`, which is a visible finding rather than a default. |
| `clusterless.node.roles` | `data`, `coordinator`, or both. Defaults to a derivation from OpenSearch's own `node.roles`. |
| `clusterless.heartbeat.interval` | default 5s. Published in the registration, and the supervisor derives this node's staleness threshold from it, so it must be the interval the node really writes at. |
| `clusterless.poll.interval` | default: the heartbeat interval. Separate because the costs are different — one is a write by every node in the fleet, the other a read of one small key. |
| `clusterless.apply` | default **false**. Whether to act on the assignment, or only to log what acting on it would do. `./gradlew run` sets it true. |
| `clusterless.retention.passes` | default 12. How many consecutive passes an index must be absent from the store before this node releases its data. A node keeps the files of an index it no longer holds a copy of, so that "stop serving this" is not "destroy this"; this is how patient the decision to finally delete them is. |
| `node.attr.remote_store.*` | not this plugin's settings, and read by it. OpenSearch's own remote-store node attributes say where this node's segments live; the plugin registers the repository they name and points segment-replication indices at it. A node with none declines search replicas, because there would be nothing for them to read. Every node holding a copy of the same shard must name the same repository — the copies share one remote path, and nothing here can check that from one node. |
| `node.attr.remote_store.mode` | `segments_only` keeps segments in the repository and the translog on local disk, and needs no translog or cluster-state repository. Right where the ingestion source holds every operation, since a transaction log is then a second copy of something already durable. It costs the ability to recover a primary from the remote store — that path restores through the remote translog — so a primary of such an index that has lost its disk is declined with a reason rather than started empty over its own data. |

`GET /_clusterless/status` reports what this node last read, applied and wrote — including
`assignment.deferred`, the copies it was told to hold and did not build, with a reason for each. That list is
the answer to the most confusing question this architecture can produce: the supervisor reports a shard as
unassigned, the node looks healthy, and nothing anywhere says the node considered the shard and declined it.

**One boundary worth knowing.** A document-replication replica needs to know which node holds its primary,
which is peer resolution, which is milestone 9 — so those copies are declined with a reason rather than built
as something else. It costs less than it sounds: the supervisor already plans `SEARCH_REPLICA` for a
segment-replication index and `REPLICA` only for a document-replication one, so the two sides agree on exactly
where the unimplemented half begins.

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

[`WORKING_NOTES.md`](WORKING_NOTES.md) is not part of that sequence and is worth reading before writing
code anyway: what OpenSearch 3.9 actually does where this plugin touches it, and what the development
sandbox does not allow. The first entry — index settings being strictly validated on the apply path rather
than ignored — is the one that would otherwise be discovered at milestone 4, as a server-side exception on
every index.

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
