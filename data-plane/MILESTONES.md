# How to build this

An order of work, not a schedule. The organising principle is the one the supervisor used and was glad of:
**publish before you read, read before you act, and act on one thing before you act on everything.** Each
milestone below leaves something an operator could look at and something a test could assert.

## Before milestone 1: the fifteen-minute orientation

Run the supervisor against a directory. It needs no store, no Docker, and no data plane:

```
cd ../opensearch-supervisor
./gradlew :supervisor-app:installDist
./supervisor-app/build/install/supervisor-app/bin/supervisor-app \
    --store=file:///tmp/dp-demo --max-passes=5 --interval=200ms --log-level=debug
```

Then look at what appeared under `/tmp/dp-demo`. That is the other half of the conversation you are about to
join, and reading its output is faster than reading its source.

Then read `../opensearch-supervisor/fake-node/src/main/java/org/opensearch/supervisor/fakenode/FakeNode.java` end to end. It is about 300
lines and it is a complete, working implementation of the node side of the contract. **You are building the same
thing with a real OpenSearch node behind it**, and the places where `FakeNode` cheats are marked.

---

## M1. A plugin that loads and can reach the store — **done**

- Plugin skeleton, `isClusterless()`, settings, `KeyValueStores.open(uri)` on start and closed on stop.
- No documents written, no state applied.
- **Done when:** a node starts with the plugin installed, logs which backend it resolved, and starts with a
  clear error naming the available schemes when the URI is wrong.

The point of stopping here is D6.2: prove nothing in the build names a backend. It is much easier to keep that
property than to recover it.

## M2. Publish: registration, heartbeat, empty inventory — **done**

The highest-value milestone by a distance, and the reason to do it second.

- Write the three documents. The inventory is empty because the node holds nothing yet.
- Honour D3.4 and D3.7 from the start: `generated_at` means "when this changed", and the declared interval is
  the real interval.

**Done when the real supervisor sees the node.** Define a pool and a tenant in the store, start a supervisor in
its default read-only mode, and it should report a live, placeable node in
`supervisor/status/pools/<pool>/convergence`. Nothing has been applied and nothing can go wrong yet, and you
have already proved the half of the contract that everything else depends on.

**It does.** `tools/supervisor-sees-me.sh` is that check, made reproducible — it publishes with the real
publisher, runs a real supervisor, and asserts `placeable_nodes`. It reports two nodes, zero unreadable
documents, and two `UNASSIGNED_PRIMARY` findings for the index that now has somewhere to go. Worth keeping
green: it is the only thing in this repository that exercises both sides of the seam at once until the
plugin can stand in for `fake-node` in `supervisor-integ`.

This is also where a **shared conformance suite** starts paying for itself — see the note at the bottom.

## M3. Read the assignment and say what you would do — **done**

Mirror the supervisor's own read-only mode, and for the same reason: this service can move data, and the safest
way to introduce it is to have it explain itself for a while first.

- Read `nodes/assignment/<node-id>` on a poll interval. No watch yet.
- Read `tenant/<t>/indices/<name>` for the indices named in it.
- Log the `ClusterState` you *would* build. Apply nothing.
- **Done when:** a supervisor in `--write` mode assigns a shard, and the node logs a complete intended state
  including index settings, mappings, roles and primary terms — without touching the node.

Build the fabrication under test here, not in M4. `NodeState.buildClusterState` is a pure function of
(previous state, desired state) and deserves to be tested as one, without a cluster.

**It is.** `tools/supervisor-assigns-me.sh` is the acceptance check, and `DataNodeStateTests` is the fabrication
tested as a pure function — including the assertions about the *second* call, which is where the expensive
mistake lives: a fabricator that looks right from an empty previous state and then restarts every shard on the
next pass. Three things this milestone settled that the plan did not name:

- **A node must consult its own disk to choose a recovery source.** The contract deliberately publishes no
  allocation ids, so nothing in the store can tell a restarting node that it used to hold a shard. Getting it
  wrong in one direction discards data; in the other it fails a recovery visibly. See `LocalShardStore`.
- **Identity travels in index settings, not in the fabricated index name.** That defers the naming question to
  a matter of legibility, which is what `IndexNaming` is now about.
- **A docrep replica is declined with a reason** rather than built as a segrep copy, and the supervisor's own
  planner draws the line in the same place.

## M4. Apply: one primary, one shard, one tenant — **written, not yet verified**

- `ClusterApplierService.updateClusterState(...)`.
- Report the copy in the inventory as it moves `INITIALIZING` → `STARTED`, with `size_bytes` appearing only
  once it is real (D3.5).
- **Done when:** a create-index task written by the control plane converges end to end — the supervisor plans a
  primary, this node creates it, and the supervisor's `tenant/<t>/status/indices/<uuid>` reports the index
  green.

Resist adding replicas, coordinators or removal here. Every one of them is easier once this loop is closed.

**Written, and the "done when" is not met yet** — deliberately stated that way rather than left ambiguous. The
applier call, the acknowledgement and the failure handling are in and unit-tested against a stand-in applier
that really runs the update function; the inventory side needs no new code, because it already reads the routing
table. What has never happened is a real `ClusterApplierService` accepting one of these states, and no node can
start in the sandbox most of this was written in.

`./gradlew run` is how it gets checked, and `plugin/build.gradle` now configures a cluster for it —
`PluginBuildPlugin` registers the `run` task but attaches no cluster, so it previously started nothing. Two
things to watch for first, both of which would fail loudly:

- **Settings validation.** Every index setting the control plane stored is validated when the index is created,
  and an unregistered one fails the index. In a deployment where the control plane's registry came from a node
  with different plugins installed, that is a real possibility rather than a hypothetical.
- **Recovery source.** An `ExistingStoreRecoverySource` paired with a freshly generated allocation id is the
  part of the fabrication with no precedent in `cluster-etcd` and no test that can prove it: whether a shard
  resumes from local data depends on the server accepting that pair. A first run on an empty node takes the
  `EmptyStoreRecoverySource` path and will not exercise it; restarting the node is what does.

## M5. Idempotence and the rest of the assignment — **the idempotence half is done**

- Multiple shards, multiple indices, multiple tenants — including the deterministic `(tenant, index-uuid)` to
  index-name mapping that the flat `ClusterState` namespace forces (see `BORROWING_FROM_CLUSTER_ETCD.md`).
- **Done when:** applying the same assignment repeatedly does not restart a serving shard (D2.2). Assert it by
  watching shard start counts across many polls, not by inspecting logs.

**Found by running a node, which no test had caught.** The shard was never restarted — D2.2 held — but the node
published a *new cluster state every second*, version climbing past 1900, each one running every
`ClusterStateApplier` and `ClusterStateListener` in the server for nothing. The cause is that the fabricator does
`version(previous.version() + 1)`, which is right, so every state differs from the last and applying
unconditionally can never reach a fixed point. `StateEquivalence` compares what this plugin actually fabricates —
index metadata and routing tables, both of which have real `equals` — and the pass applies only on a difference.
Worth remembering as a shape: "idempotent" is not the same as "reaches a fixed point", and only the second one
is visible from outside.

The multiple-shards, multiple-indices, multiple-tenants half needs no new code — the fabricator loops and the
naming is deterministic per `(tenant, uuid)` — but it has not been exercised against a real node yet.

## M6. Removal — **started: the data-loss half is settled**

- A shard absent from the assignment is dropped (D2.3), and local data survives long enough to be reported
  (D5.4).

**The mechanism D5.4 needs turns out to be a choice the server makes for you, and getting it wrong deletes
data.** `IndicesClusterStateService` picks how to release an index from *why* it went away, and
`ClusterChangedEvent.indicesDeleted()` decides that purely by whether the metadata is still in the state:

| reason | triggered by | what happens to the files |
|---|---|---|
| `DELETED` | index in the previous metadata, absent from the new one | **removed** |
| `NO_LONGER_ASSIGNED` | metadata still present, no shards on this node | kept "in the case of a disaster recovery" |
| `CLOSED` | metadata says `close` | kept |

So dropping a copy must keep the index's metadata and remove only its routing entry, which is what
`DataNodeState.retainIndicesNoLongerWanted` does — carried forward from the previous state, so it stays a pure
function and a restart forgets it harmlessly.

This also caught a bug already in the tree: the resolver dropped a **closed** index from the desired state
entirely, which reads as a deletion. Closing an index would have deleted its data and left nothing to reopen.

Still to do here: the second half of a move against a real node, and reaping the retained metadata once an index
really is gone from the store — the supervisor's `ORPHANED_COPY` discipline turned inward, deliberately not
guessed at yet.
- **Done when:** the supervisor's rebalancer completes a move. Its `SurplusReaper` drops the source copy by
  removing it from that node's assignment; this milestone is what makes the second half of a move actually
  happen. `../opensearch-supervisor/DESIGN.md` §14.5 explains why the two halves are deliberately unaware of
  each other, and it is worth reading before implementing this one.

## M7. Replicas, and waiting for a peer

- Search replicas for segment replication.
- D5.3 is the requirement that bites: a replica placed in the same pass as its primary must **wait**, not fail.
- **Done when:** an index with one replica goes green, and a test that places both copies in one pass does not
  produce a failed shard.

## M8. Coordinators

- Route from `tenant/<t>/status/indices/<uuid>`, resolve addresses from `tenant/<t>/status/nodes/<id>`.
- **Done when:** a search against a coordinator that holds no shards returns results from data nodes.

This is the milestone that makes the system *usable* rather than merely converged, and it is the definition of
"complete" the supervisor's task model deliberately does not yet use — worth noting when you get here.

## M9. Document replication

- Peer resolution, primary term handling, push-based ingestion.
- **Done when:** an index with `index.replication.type: DOCUMENT` indexes and replicates.

Expect D8.2 to be in the way. Read it before starting, and treat what you learn as a finding.

## M10. Cluster-manager actions — **the not-hanging half is done**

- `cluster.health` and `cluster.state` answered from the projections; mutations refused with a reason naming
  the control plane.
- **Done when:** the client libraries people actually use do not hang.

**Brought forward, because it was blocking basic use.** Indexing a document waited thirty seconds and answered
`cluster_manager_not_discovered_exception` with a null reason — the write needed a mapping change, which goes to
the cluster manager. `ClusterManagerActionFilter` now marks read requests local and refuses everything else that
needs a manager, immediately, with a message naming the control plane.

What is still missing is the half this milestone is really about: health and state are answered from **local**
state, which is one node's view where a client asking about a cluster expects the fleet's. The supervisor already
projects the fleet's view into `tenant/<t>/status/cluster` and `tenant/<t>/status/indices/<uuid>`; serving those
is the remaining work, and it needs a tenant to answer for — which is a question the current filter does not even
ask.

## M11. Watch

Last, deliberately. The poll loop must already be correct (D2.1), so this is purely latency.

- **Done when:** a test with every notification dropped still converges (D9.4), and one with notifications
  converges faster.

---

## The shared conformance suite — **done, at M2 as recommended**

`NodeContractTests` and its `ContractNode` adapter live in
`../opensearch-supervisor/fake-node/src/testFixtures`, published as test fixtures and consumed here in test
scope. Thirteen assertions, both implementations passing. Two notes for whoever extends it:

- **It reads with the supervisor's parser, never with each implementation's own.** A suite that let each
  side check its own JSON would pass happily on two mutually unreadable documents, which is the failure it
  exists to prevent.
- **The adapter is narrow on purpose.** Everything in it is something both implementations can honestly do.
  `fake-node` can only hold a copy by being assigned it and the plugin is handed one directly, so the suite
  asks for "hold these copies" and says nothing about how. Anything one side can do and the other cannot
  belongs in that side's own tests rather than behind a capability flag — a conformance suite half its
  implementations skip proves nothing about either.

The original note follows.

## The shared conformance suite: why

`NODE_CONTRACT.md` will shortly have two independent implementations: `fake-node` and this plugin. Two
implementations of one document format tested separately are two formats that will drift.

The recommendation (D9.1) is to extract the contract assertions into a suite both run, in the manner of
`KeyValueStoreContractTests` in `../opensearch-kv-store/store-api/src/testFixtures`. It belongs in the
**supervisor** repository, which owns the contract, published as test fixtures for this one to consume — the
same arrangement the store SPI already uses and which both other services already depend on.

Worth doing around M2, when there is exactly one document's worth of assertions to move and the shape is
obvious. It gets harder every milestone after that.

## Reporting back

The supervisor's specifications are not sacred and three of them have already been corrected by being
implemented. Where `NODE_CONTRACT.md` is silent, wrong, or asks for something a real OpenSearch node cannot
cheaply provide, that is a change to make there rather than a workaround to bury here. The document has a
section for exactly this — *"Checked against a real data plane"* — recording what the previous survey confirmed,
corrected and warned about. Add to it.
