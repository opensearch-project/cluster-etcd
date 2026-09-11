# What to take from `cluster-etcd`, and what to leave

A survey of `../cluster-etcd/plugin`, written by reading it against the requirements here. The split is
roughly: **how to talk to OpenSearch is solved and should be lifted; what to read and where from is not, and
almost none of it survives.**

Read this before writing code, and treat the "lift" column as the reason this project is weeks rather than
months.

## Summary

| file | verdict |
|---|---|
| `changeapplier/ChangeApplierService.java` | **Lift.** The whole trick, in 40 lines. |
| `ClusterETCDPlugin.java` | **Lift the extension points**, discard the etcd wiring. |
| `ClusterManagerActionFilter.java` | **Lift the idea**, rewrite the bodies. |
| `changeapplier/DataNodeState.java` | **Adapt.** The hard-won part; the input shape changes. |
| `changeapplier/CoordinatorNodeState.java` | **Adapt**, same. |
| `changeapplier/ClusterStateUtils.java` | **Lift.** Small and non-obvious. |
| `changeapplier/NodeState.java`, `NodeStateApplier.java` | **Lift.** Interfaces worth keeping. |
| `ETCDHeartbeat.java` | **Adapt the scheduling**, replace the document. |
| `ETCDWatcher.java` | **Adapt the cooldown**, replace the mechanism. |
| `ETCDStateDeserializer.java` | **Discard.** Different documents entirely. |
| `ETCDPathUtils.java` | **Discard.** Different key layout entirely. |
| `ETCDClientHolder.java` | **Discard.** Replaced by the SPI. |

---

## Lift

### `ChangeApplierService` — the mechanism

This is the whole idea and it is very small:

```java
clusterApplierService.updateClusterState(source, s -> nodeState.buildClusterState(s, indicesService), this::logError);
```

That is the "voice in its head": a locally fabricated `ClusterState` handed to the same applier the real
publication path uses, so recovery, allocation-awareness, search routing and everything else downstream runs
completely unmodified. Nothing about it is etcd-specific. **Do not reimplement what the server does with a
cluster state** — the value here is that this one call means you do not have to.

`removeNode` building from `ClusterState.EMPTY_STATE` is the shutdown path and is worth keeping too.

### `ClusterETCDPlugin` — the extension points

The list of hooks is the reusable part, and finding them is most of the work:

- **`ClusterPlugin.isClusterless()` returning true.** An actual upstream extension point for exactly this.
  Worth knowing it exists before going looking for something to subvert.
- **`getExecutorBuilders`** — dedicated thread pools for the watcher and the heartbeat, so neither competes
  with search.
- **`getActionFilters`** — where cluster-manager actions get intercepted.
- **`getSettings`** — but see D6.1: one store *URI*, not an endpoint.
- **`close()`** shutting the watcher and client down in order.
- **`GuiceHolder`** — a lifecycle component whose only job is to capture `IndicesService`, which
  `createComponents` cannot otherwise reach and which mapping canonicalisation needs. Ugly, and load-bearing.
  Check whether current OpenSearch offers a cleaner path (open question 4) before copying the hack; copy the
  hack if it does not.

### `ClusterStateUtils`

Two small functions with more thought in them than their size suggests, including a **deterministic creation
date derived from the index name**. That matters: `IndexMetadata` demands a creation date, and generating one
from the clock would produce different metadata on every node and every restart, which is a difference that
shows up as spurious cluster-state churn. Take it.

### `NodeState` / `NodeStateApplier`

A two-method interface and an abstract base with one abstract `buildClusterState`. The separation is right —
one implementation per node role, one applier — and it makes the fabrication unit-testable without a cluster.
Keep the shape.

---

## Adapt

### `DataNodeState.buildClusterState` — the genuinely difficult code

Keep the *structure*, replace the *input*. The structure encodes several things that are easy to get wrong and
tedious to rediscover:

- `version(previousState.version() + 1)`, so each application looks like a newer publication.
- `DiscoveryNodes` containing **only the local node** (plus peers where docrep needs them), with
  `localNodeId` set. There is no cluster to enumerate.
- Carrying `version`, `settingsVersion` and `mappingVersion` **forward from the previous state** when the index
  already existed, rather than resetting them. Resetting makes the server think everything changed.
- `primaryTerm` per shard.
- Mappings routed through `IndicesService` to canonicalise them, so that byte-identical intent produces
  byte-identical metadata.
- Building `IndexShardRoutingTable` against the *previous* routing table, so a copy that is already started
  stays started. This is what satisfies D2.2, and getting it wrong means every poll restarts every shard.

What changes: the input is no longer `cluster-etcd`'s goal-state document. It is
`nodes/assignment/<node-id>` — a complete desired set of `(tenant, index-uuid, shard) -> role` — plus
`tenant/<t>/indices/<name>` for settings and mappings, plus the status projections for peers. So the loop that
walks `indices` stays; what fills `indices` is new.

One difference worth planning for: this design is **multi-tenant**, and `cluster-etcd` is not. A single node
can hold shards for several tenants, and index names are only unique within a tenant. The fabricated
`ClusterState` has one flat index namespace, so **you need a deterministic mapping from `(tenant, index-uuid)`
to the index name the server sees**, and it must be stable across restarts. Do not use the bare index name.

### `ETCDHeartbeat`

The *scheduling* is reusable: a dedicated pool, a fixed interval, capacity gathered from the node. Note the
detail of reading `http.port` from settings for the published address — the real address matters because
coordinators resolve peers by it.

The *document* is entirely different. `cluster-etcd` writes one heartbeat carrying everything, which is
precisely what `NODE_CONTRACT.md` splits into three documents to avoid (D3.1–D3.3). In particular: this
plugin's heartbeat carries the inventory's **digest**, not the inventory, and the inventory is written **only
when it changes** (D3.4).

### `ETCDWatcher`

The **1-second event cooldown, applying only the last seen change**, is a good idea worth keeping under any
notification mechanism: assignments can change several times in a burst and applying each in turn is wasted
recovery work.

The mechanism changes completely. `Watch.Watcher` is etcd's API; the SPI's `watch` is optional and may be
absent, so the plugin needs a **poll loop that is correct on its own**, with watch as a latency optimisation
layered on top (D2.1, D9.4). Structuring it the other way round — a watcher with polling bolted on as a
fallback — is how a correctness property becomes dependent on a notification that backends are allowed to drop.

---

## Discard

### `ETCDStateDeserializer` and `ETCDPathUtils`

The key layout and every document format are different, deliberately: `KEY_LAYOUT.md` was designed after
`cluster-etcd` and fixes specific problems with it, including keying on index **uuid** rather than name, and
grouping node documents **by kind rather than by node** so a prefix scan can fetch one kind across all nodes
without skipping a middle segment.

There is one thing to carry across as a warning rather than as code. `cluster-etcd`'s heartbeat put peer node
**names** in the goal state and resolved them to addresses by *reading that node's heartbeat*, which made every
data node a reader of every other data node's document. `NODE_CONTRACT.md` rejects that for two stated reasons —
it couples the data plane to the heartbeat schema, and **names are reused while ids are not**, so a peer
resolved by name eventually points at a node's successor. Peers come from `tenant/<t>/status/nodes/<id>`
instead. If you find yourself reading another node's heartbeat, something has gone wrong.

### `ETCDClientHolder`

Replaced wholesale by `KeyValueStores.open(uri)`. The lifecycle concern it encodes — one client for the node,
closed on shutdown — remains real; the SPI just supplies the client.

---

## One correction to carry back

`cluster-etcd`'s heartbeat publishes the **entire cluster routing table** from every node. That was noted as a
scaling hazard when this design was being written, and the correction is worth restating because it shapes what
the plugin needs to read: **in a clusterless model a data node knows only its own shards** — and, under
document replication, the immediate peers of those shards. It has no business knowing the fleet's routing
table, and needing it would be a design problem rather than a data problem.

A coordinator does need placements, but only for **the tenants it serves**, which is what
`tenant/<t>/status/indices/<uuid>` is scoped to.
