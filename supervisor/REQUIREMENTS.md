# OpenSearch supervisor — requirements

The management plane: the service that makes reality match the control plane's intent.

Written before any code exists, from the three-plane model the control plane already assumes. Where a
requirement is a decision rather than a given, it says so, because the ones worth arguing with are the ones
that will shape everything else.

## 1. What this service is

**One sentence:** read desired state from the shared store, read actual state from data plane heartbeats,
and issue instructions until the two agree.

```
                  ┌─────────────────┐
                  │  control plane  │   short-lived CRUD; writes intent
                  └────────┬────────┘
                           │ writes
                  ┌────────▼────────┐
                  │  shared KV store │  ◄──── the only interface between planes
                  └────┬───────┬─────┘
              reads    │       │   reads/writes
                  ┌────▼───────▼────┐
                  │   supervisor    │   this service: reconcile
                  └────────┬────────┘
                           │ writes assignments; reads heartbeats
                  ┌────────▼────────┐
                  │   data plane    │   OpenSearch nodes + a plugin
                  └─────────────────┘
```

**No plane calls another directly.** The control plane already holds to this — it has exactly one outbound
seam, `KeyValueStore` — and the supervisor must too. If a requirement here seems to want an RPC to the
control plane, the requirement is wrong.

### The lifetime rule that decides scope

The control plane's rule is "if an operation cannot finish inside its own request, it does not belong
here". The supervisor is the other side of that: **everything whose completion outlives a single request
belongs to the supervisor.** Concretely, work already identified as its:

- shard allocation and rebalancing (the "shard manager")
- the task lifecycle the control plane leaves half-done — see §4
- health computation, which the control plane cannot answer today
- snapshot repository cleanup, cloning, verification, and snapshot metadata projection
- node decommissioning, which is a drain

## 2. The reconciliation loop is the whole design

Everything else is detail. The loop:

1. **Observe desired state** — `scanPrefix` over the control plane's keys (see `KEY_LAYOUT.md`).
2. **Observe actual state** — the most recent heartbeat from each data plane node.
3. **Diff** — compute what differs, as data.
4. **Plan** — turn the diff into an ordered set of instructions.
5. **Act** — write instructions where the data plane will read them.
6. **Repeat.**

Non-negotiable properties, in rough order of how much pain their absence causes:

- **R2.1 Level-triggered, not edge-triggered.** Reconciliation must be a pure function of observed state,
  never of an event that happened. A supervisor that acts on "a task appeared" will not converge state that
  predates it, or state whose task it consumed before crashing. Tasks are a *hint about where to look*, and
  the loop must produce the same result with the queue empty. This is the single most important requirement
  in this document and the easiest to violate by accident.
- **R2.2 Idempotent.** Running the loop twice over unchanged input must issue nothing the second time.
- **R2.3 Interruptible at any point.** The process can die between any two steps. There must be no state
  held only in memory whose loss leaves the system stuck.
- **R2.4 Convergent, not merely correct.** Each pass must move measurably closer; oscillation between two
  states that each look locally right is the characteristic failure of this kind of service and must be
  detectable (see R7).
- **R2.5 Tolerant of torn desired state.** The control plane writes per key with no cross-key transaction,
  so the supervisor **will** observe an index without its alias, or a data stream naming a backing index
  whose document has not landed. It must treat this as normal and retry, never as corruption.
- **R2.6 Bounded work per pass.** A pass over ten thousand indices must not be one giant write. Batching
  and a per-pass ceiling, so that a large backlog makes progress rather than timing out forever.

## 3. Actual state: heartbeats

**Nothing in the store describes reality today.** The heartbeat channel does not exist and defining it is
this project's first real design task — it is the reason `cluster.health`, `cluster.state` and
`indices.get_data_stream`'s `status` are all still unimplemented in the control plane.

- **R3.1** A node periodically writes a heartbeat to the shared store — under a supervisor-owned prefix,
  not the control plane's. Suggested: `nodes/<node-id>/heartbeat`.
- **R3.2** A heartbeat reports what that node *has*, not what it was told: shard copies present and their
  state, the configuration version it has applied, disk and load figures allocation needs, and its
  attributes (the awareness values weighted routing already validates against).
- **R3.3 Liveness is derived from staleness**, not from a node saying goodbye. A heartbeat carries a
  timestamp; a node whose heartbeat is older than a threshold is presumed gone. Getting this threshold
  wrong in either direction is how these systems cause outages, so it must be configurable and its
  effective value observable.
- **R3.4 Heartbeats are the *only* source of actual state.** No shard-level RPC from the supervisor to a
  node. This preserves the one-way-store property and means the supervisor can be restarted, moved, or run
  redundantly without the data plane noticing.
- **R3.5** A node's heartbeat must be small and bounded — a cluster of 200 nodes writing every few seconds
  is the store's dominant write load, and both backends cap value size.

**Open question worth settling early:** does the supervisor also need a *per-node desired* document
(`nodes/<node-id>/assignment`) that the node polls, or does the node derive its work from the index
documents plus a global assignment map? A per-node document is more writes but far less logic in the data
plane plugin, and it makes "what was this node told?" directly inspectable. I would start there.

## 4. The task lifecycle — the concrete handoff that exists today

The control plane writes `tasks/index-management/<uuid>` records and **never touches them again**. Nothing
sets `state` to anything but `pending`; nothing deletes them. `cluster.pending_tasks` reports the pile
faithfully and it grows without bound. This is a known, deliberate gap, documented as the supervisor's
half.

- **R4.1** Claim a task before working it, so two supervisor instances cannot both act. A lease with an
  expiry, not a lock — the holder can die.
- **R4.2** Record attempts, and stop retrying eventually. A task that has failed many times must become
  visible rather than looping silently.
- **R4.3** Move a task to a terminal state and reap it, so the queue drains and `cluster.pending_tasks`
  starts telling the truth.
- **R4.4** Tolerate duplicates and losses. See R2.1: a task is a hint. Correctness must not depend on
  each one being seen exactly once.
- **R4.5** The record's `sequence` field is per control plane process and restarts at zero. **Do not use it
  for global ordering.** `submitted_at` is millisecond resolution with frequent ties. If the supervisor
  needs a total order it must establish one itself.

## 5. What the supervisor writes back

Some of the control plane's unimplemented operations are blocked on the supervisor projecting facts into
the store. Each is a small, well-defined deliverable and worth doing early, because each unblocks a
user-visible feature:

- **R5.1 Health.** Enough for `cluster.health`, and for `indices.get_data_stream`'s required `status`
  field. Per index and cluster-wide.
- **R5.2 Node and shard state**, enough for `cluster.state`.
- **R5.3 Snapshot metadata**, projected from repository reads, so `snapshot.get` and `snapshot.status` can
  be served without the control plane growing a second outbound seam. This was an explicit control plane
  scope decision, not an afterthought.
- **R5.4** Live node attribute values, which would let the control plane enforce the half of weighted
  routing validation it currently cannot — the "discovered awareness values" check.

**R5.5 The supervisor must not write into the control plane's key space.** Its own prefixes only. Anything
it projects for the control plane's benefit is at an agreed, documented path, and adding one is a change to
both services rather than a unilateral write.

## 6. Correctness under multiple instances

- **R6.1** Running two supervisors must be safe, even if only one is useful. Leader election or per-task
  leases; both backends can support either.
- **R6.2** Prefer **compare-and-set on the specific key** to a global lock. The store's revisions exist for
  this and it degrades better: a stale supervisor loses individual writes rather than corrupting a shared
  lock.
- **R6.3** A supervisor that loses its lease and later comes back must not apply a stale plan. Plans are
  cheap; recompute rather than resume.

## 7. Observability, and why it is a requirement rather than a nicety

A reconciler that is quietly failing looks exactly like one with nothing to do. Both produce no output.

- **R7.1 Expose the diff.** An operator must be able to ask "what does this think is wrong right now?" and
  get the actual list. This is the single most useful thing the service can offer.
- **R7.2 Expose convergence**, not just liveness: how many passes, how long since the diff was empty,
  whether it is shrinking. A diff that is stable and non-empty is the alarm.
- **R7.3 Detect oscillation** — the same item appearing in the diff pass after pass while the supervisor
  keeps acting on it — and say so loudly.
- **R7.4** Log the *decision*, not just the action. "Moving shard X to node Y **because** node Z has been
  silent for 40s" is debuggable; "moving shard X to node Y" is not.

## 8. Sharing the key/value store code

The store is already extracted into `:store-api` in the control plane repo — a module with **no
dependencies at all**, holding the `KeyValueStore` interface, the `KeyValueStoreProvider` SPI, the
filesystem implementation, and (as test fixtures) `InMemoryKeyValueStore` plus `KeyValueStoreContractTests`.
The etcd and ZooKeeper backends are separate jars, each verified against a real server.

**The supervisor must use exactly this code, not a reimplementation.** Two implementations of the same
revision semantics against the same keys is how the planes would silently disagree.

**Done.** It now lives at [`../opensearch-kv-store`](../opensearch-kv-store) as its own build and its own
git repository, consumed by the control plane as a Gradle **composite build**. Add the same two lines here:

```kotlin
// settings.gradle.kts
includeBuild("../opensearch-kv-store")

// build.gradle.kts
implementation("org.opensearch.controlplane:store-api:0.1.0-SNAPSHOT")
testImplementation(testFixtures("org.opensearch.controlplane:store-api:0.1.0-SNAPSHOT"))
```

Substitution matches on **group and name**, so keep `group = "org.opensearch.controlplane"` and that
version, or the coordinate will resolve against a repository instead of the local project and fail.

`testFixtures(...)` is how you get `InMemoryKeyValueStore` and `KeyValueStoreContractTests` — worth taking
from day one, so the supervisor's own tests run against the same in-memory store the control plane's do.

The cost, recorded so nobody rediscovers it: each consumer's build depends on a path outside itself, so CI
must check out both repositories. A submodule or a published artifact removes that later without changing
the layout.

**What is already there**, so the supervisor need not build any of it:

| | |
|---|---|
| interface | `KeyValueStore` — get, prefix scan, conditional put and delete, optional watch, close |
| backends | etcd and ZooKeeper, both verified against real servers; filesystem for local runs |
| watches | native on both remote backends; `Watches.watchOrPoll` hides the difference |
| contract suite | `KeyValueStoreContractTests` as a test fixture — extend it for any new backend |
| in-memory store | `InMemoryKeyValueStore`, same fixture; use it for the supervisor's own unit tests |
| integration tests | Testcontainers, so CI needs only Docker; `WORKING_NOTES.md` has the pattern |

### Watches — available, and deliberately weak

`KeyValueStore.watch(prefix, onChange)` exists. Use it through the helper, which is what makes it work on
every backend:

```java
try (Watch watch = Watches.watchOrPoll(store, "tenant/default/indices/", Duration.ofSeconds(2), loop::wake)) {
    …
}
```

**A notification carries no payload** — no changed key, no value, no revision, no ordering. It means only
"something under your prefix may have changed; re-read it". Notifications may be coalesced, spurious, or
missed entirely. That is the contract, not an implementation shortcoming, and it was chosen for R2.1's
benefit: an API that handed over changed keys would invite applying just those, which is correct until the
first missed notification and then silently is not.

So a watch is **a way to stop polling so often, never a way to avoid re-reading.** The supervisor's loop
should be exactly the same loop with or without one; the watch only decides how soon it next runs.

Backends, transparently behind `watchOrPoll`:

| backend | how |
|---|---|
| ZooKeeper | native, `AddWatchMode.PERSISTENT_RECURSIVE` — one registration covers a subtree, no re-arming |
| etcd | native range watch |
| filesystem | polling |

Both remote backends therefore watch natively, and `Watches.pollFailures()` only counts anything if you
are on the filesystem store. `EtcdKeyValueStore.watchFailures()` is the etcd equivalent.

`Watches.pollFailures()` counts polls that threw. Publish it: a silently dead watch looks exactly like a
quiet keyspace, which is this mechanism's most likely failure.

## 9. Explicit non-goals

- **Not serving the OpenSearch API.** That is the control plane's, and it is nearly complete.
- **Not holding data.** Shards live on data nodes.
- **Not a scheduler for user work** — no search or indexing traffic passes through here.
- **Not a second source of truth.** If the supervisor and the store disagree about intent, the store wins.

## 10. Suggested first milestone

Small enough to finish, large enough to prove the design:

1. Extract the KV store to its own directory; both repos build against it. (§8)
2. Define and implement the heartbeat document, with a fake data plane node that writes one.
3. Read desired state (`indices/`) and actual state (heartbeats); compute a diff; **expose it and do
   nothing else.** A read-only reconciler that can already answer "what is wrong" is the most valuable
   thing here and is far easier to trust than one that acts.
4. Only then act — starting with the narrowest case: an index in the store with no shard copies anywhere.
5. Close the task lifecycle (§4) so `cluster.pending_tasks` stops growing for ever.

Step 3 before step 4 is deliberate. A reconciler that observes correctly and acts on nothing is safe and
immediately useful; one that acts before it observes correctly is neither.
