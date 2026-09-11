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

## M1. A plugin that loads and can reach the store

- Plugin skeleton, `isClusterless()`, settings, `KeyValueStores.open(uri)` on start and closed on stop.
- No documents written, no state applied.
- **Done when:** a node starts with the plugin installed, logs which backend it resolved, and starts with a
  clear error naming the available schemes when the URI is wrong.

The point of stopping here is D6.2: prove nothing in the build names a backend. It is much easier to keep that
property than to recover it.

## M2. Publish: registration, heartbeat, empty inventory

The highest-value milestone by a distance, and the reason to do it second.

- Write the three documents. The inventory is empty because the node holds nothing yet.
- Honour D3.4 and D3.7 from the start: `generated_at` means "when this changed", and the declared interval is
  the real interval.

**Done when the real supervisor sees the node.** Define a pool and a tenant in the store, start a supervisor in
its default read-only mode, and it should report a live, placeable node in
`supervisor/status/pools/<pool>/convergence`. Nothing has been applied and nothing can go wrong yet, and you
have already proved the half of the contract that everything else depends on.

This is also where a **shared conformance suite** starts paying for itself — see the note at the bottom.

## M3. Read the assignment and say what you would do

Mirror the supervisor's own read-only mode, and for the same reason: this service can move data, and the safest
way to introduce it is to have it explain itself for a while first.

- Read `nodes/assignment/<node-id>` on a poll interval. No watch yet.
- Read `tenant/<t>/indices/<name>` for the indices named in it.
- Log the `ClusterState` you *would* build. Apply nothing.
- **Done when:** a supervisor in `--write` mode assigns a shard, and the node logs a complete intended state
  including index settings, mappings, roles and primary terms — without touching the node.

Build the fabrication under test here, not in M4. `NodeState.buildClusterState` is a pure function of
(previous state, desired state) and deserves to be tested as one, without a cluster.

## M4. Apply: one primary, one shard, one tenant

- `ClusterApplierService.updateClusterState(...)`.
- Report the copy in the inventory as it moves `INITIALIZING` → `STARTED`, with `size_bytes` appearing only
  once it is real (D3.5).
- **Done when:** a create-index task written by the control plane converges end to end — the supervisor plans a
  primary, this node creates it, and the supervisor's `tenant/<t>/status/indices/<uuid>` reports the index
  green.

Resist adding replicas, coordinators or removal here. Every one of them is easier once this loop is closed.

## M5. Idempotence and the rest of the assignment

- Multiple shards, multiple indices, multiple tenants — including the deterministic `(tenant, index-uuid)` to
  index-name mapping that the flat `ClusterState` namespace forces (see `BORROWING_FROM_CLUSTER_ETCD.md`).
- **Done when:** applying the same assignment repeatedly does not restart a serving shard (D2.2). Assert it by
  watching shard start counts across many polls, not by inspecting logs.

## M6. Removal

- A shard absent from the assignment is dropped (D2.3), and local data survives long enough to be reported
  (D5.4).
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

## M10. Cluster-manager actions

- `cluster.health` and `cluster.state` answered from the projections; mutations refused with a reason naming
  the control plane.
- **Done when:** the client libraries people actually use do not hang.

## M11. Watch

Last, deliberately. The poll loop must already be correct (D2.1), so this is purely latency.

- **Done when:** a test with every notification dropped still converges (D9.4), and one with notifications
  converges faster.

---

## The shared conformance suite

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
