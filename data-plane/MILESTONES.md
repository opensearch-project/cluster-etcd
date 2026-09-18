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

### Added later: a node rewrites its registration when it goes missing

Not part of the original milestone, and it belongs here because this is the component that owns the document. The
supervisor's `DESIGN.md` §20.2 declined to reap a departed node's four documents, for a reason worth keeping: three
of them belong to the node, and deleting the registration of a node that is merely *partitioned* rather than dead
leaves it heartbeating with no registration — `NODE_UNREGISTERED`, placeable for nothing, and needing a restart to
escape. Trading a slow leak for a node that strands itself is not an improvement.

The fix it named, and asked this side to agree: **a node rewrites its registration if it observes the document
missing.** `NodePublisher` now reads the key on a pass where it has nothing else to write, and writes the same
document again when it is gone — same `ephemeral_id`, because the process has not restarted, and the same
`registered_at`, because the document is new and the registration is not. That last point also settles
`NODE_CONTRACT.md`'s own finding 4, which had raised the question for the attribute-change case and left it open.

Three things the tests pin, and the middle one is the trap:

- a registration deleted underneath the node is back within one pass, with its identity intact;
- **a quiet pass checks and does not write.** Rewriting whenever the document is merely present would put the
  coldest of the three documents back on the hot path — a write per pass per node — and the revision advancing
  every pass is exactly what the supervisor's liveness check reads as activity. This is the D3.4 mistake in a new
  place, and the version that makes the rule worse than not having it;
- a read that *fails* is not an absent document. Same shape as "an absent assignment is not an empty one": the node
  reports it and decides again next pass, and the heartbeat still goes out, because a node that stopped
  heartbeating over this would have its shards rebuilt elsewhere.

**Self-healing hides the thing it heals**, so the node counts the rewrites and reports them at
`/_clusterless/status` as `registration.rewrites_after_going_missing`, rendered only when non-zero. One is an
incident; a steady trickle is something reaping live nodes' registrations, and without the counter the node would
paper over that for ever.

**What it does not cover:** absence only. Something that *overwrites* the registration with different content goes
unnoticed, because the comparison that decides whether to write is against this process's in-memory copy and the
store read only asks whether the key exists. The layout's one-writer rule says that cannot happen, so it is a gap
in detection rather than in behaviour — filed as such with the supervisor, because "the node self-heals its
registration" reads stronger than it is.

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

## M4. Apply: one primary, one shard, one tenant — **done, and seen working**

- `ClusterApplierService.updateClusterState(...)`.
- Report the copy in the inventory as it moves `INITIALIZING` → `STARTED`, with `size_bytes` appearing only
  once it is real (D3.5).
- **Done when:** a create-index task written by the control plane converges end to end — the supervisor plans a
  primary, this node creates it, and the supervisor's `tenant/<t>/status/indices/<uuid>` reports the index
  green.

Resist adding replicas, coordinators or removal here. Every one of them is easier once this loop is closed.

**Done, with a control plane, a supervisor and a data node sharing one store.** The supervisor plans a primary,
this node applies it, the shard recovers to `STARTED` with a real `size_bytes`, and the index can be searched and
written to. Closing it releases the shard without deleting the files; reopening recovers with `EXISTING_STORE`,
which closes the one question the fabrication could not answer by itself.

The "done when" asked for the index to be reported **green**, and it reaches yellow. That is not a defect here: it
was created with one replica and document replication, and a docrep replica needs peer resolution (M9). Green
needs zero replicas, or segment replication across two nodes (M7).

**Five bugs came out of the first real run, and four were of one kind.** Worth reading the list before writing
anything else that runs on a pass:

- an `INFO` log inside `buildClusterState` — one line per poll interval, for ever;
- a `NullPointerException` in the mapping merge, on an index created with no mappings;
- that exception aborting the *whole* fabrication, so one index's problem stopped the node entirely;
- `createIndexMapperService` called every pass — expensive, `synchronized`, and it fires every installed plugin's
  `onIndexModule` hook, which logged two lines a second from unrelated components and read exactly like a shard
  restart loop;
- and, before those, applying a new cluster state every pass because the version always differs.

The pattern: **the fabricator is a pure function called speculatively on every pass**, so anything expensive,
observable or fallible inside it happens far more often than the code suggests — and when the observable thing is
another component's log line, the symptom points anywhere but here.

The one prediction that held: index settings the node has not registered fail the index at creation. Nothing hit
it, because the control plane's registry came from a node with the same plugins.

### The in-sync set grew without bound, and it killed a node

Found by an operator deleting and recreating an index enough times, and the failure is a fatal `AssertionError` out
of the cluster applier thread, which exits the node:

```
In absence of remote translog store, all tracked shards must have replication mode as LOGICAL_REPLICATION
  at ReplicationTracker.calculateReplicationGroup(ReplicationTracker.java:1114)
  at ReplicationTracker.updateFromClusterManager(...)
  at IndexShard.updateShardState(...)
```

The fabricated `in_sync_allocation_ids` carried the previous pass's set forward wholesale and added the current
allocation id. `routingFor` mints a fresh allocation id whenever the previous entry cannot be reused — a role
change, or a copy that was not started or initializing — so **every id a shard had ever used stayed in the set for
ever**, describing copies that no longer existed in the routing table.

Why that is fatal rather than untidy, read from the server: `ReplicationTracker.updateFromClusterManager` puts a
`CheckpointState` in for **every** in-sync id, and marks it `replicated` only when the id is the primary's —
because `assignedToRemoteStoreNode` is true for a remote-store-backed index, and `isReplicated` then returns true
for the primary and its relocation target only. `calculateReplicationGroup` asserts that, absent a remote translog,
every *tracked* copy is `replicated`, and its four escapes all fail here:

- the first replication group is exempt, and this is not the first;
- `isRemoteTranslogStoreEnabled()` is false — that is what `segments_only` means;
- the migration escape wants a replication target on a node where `DiscoveryNode.isRemoteStoreNode()` holds, and
  that predicate requires a **cluster-state** repository as well as a segment one, which a `segments_only` node does
  not have;
- so it comes down to "every tracked copy is replicated", and one stale id is enough to break it.

**The invariant this deployment has been living inside, unknowingly:** on a `segments_only` node, a
remote-store-backed shard's in-sync set may contain the primary's allocation id and nothing else. It held by luck
while nothing churned a copy's role.

The fix is that the set describes this pass's copies and carries nothing forward, which is also what the field
means. Two tests: the accumulation, reproduced through a role change and failing with two ids where one belongs, and
the stability property that made the carry-forward look reasonable in the first place — the id must not churn
either, or the server holds a routing entry whose allocation id its own metadata does not believe in.

**Worth carrying into M9**, because this is the same machinery that milestone needs: document replication puts real
replicas in the replication group, and a docrep replica has to be both tracked *and* replicated. The assertion above
is the server telling us what shape the in-sync set has to have, and it will have more to say when the copies are no
longer all remote-store-backed.

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

**Verified against a real node**, and further than expected: a restart with `--preserve-data` recovers the shard
with `EXISTING_STORE` and the document written before the restart is still searchable, which closes D2.5's
returning-node case properly rather than by the close/open shortcut. Deleting the index from the control plane
leaves the metadata in place, as intended.

**Reaping is now implemented.** `RetainedIndexReaper` reads `tenant/<t>/indices/<name>` for each index the node is
retaining — it has the tenant and the name from the fabricated settings — and drops the metadata, and therefore the
files, once the document has been absent for `clusterless.retention.passes` consecutive passes. The uuid is checked
too, since a document under the same name may describe a different index, in which case the retained one is gone.

Three properties are worth keeping, and each has a test:

- **Doubt is not evidence.** A store that cannot be read and a document that will not parse both reset the count.
  Treating silence as deletion is how a network blip becomes data loss.
- **The count resets on any clear look**, so a copy that comes and goes cannot accumulate strikes across the gaps.
- **The counting happens in the poller, once per pass** — never in the fabricator, which is called twice on a pass
  that applies and would reach the threshold in half the intended time. That is the fourth bug of that shape,
  avoided rather than discovered.

Still to do: the second half of a move — the supervisor's `SurplusReaper` dropping a source copy — has never run.
And **a restart still orphans retained data**: the retained set comes from the previous cluster state, so a new
process has nothing to reap and the files sit there until somebody removes them. Closing that means reading the
node's data directory rather than its cluster state, which is the dangling-index problem OpenSearch has its own
machinery for, and is deliberately not attempted yet.
- **Done when:** the supervisor's rebalancer completes a move. Its `SurplusReaper` drops the source copy by
  removing it from that node's assignment; this milestone is what makes the second half of a move actually
  happen. `../opensearch-supervisor/DESIGN.md` §14.5 explains why the two halves are deliberately unaware of
  each other, and it is worth reading before implementing this one.

## M7. Replicas, and waiting for a peer — **done, and confirmed on two real nodes**

- Search replicas for segment replication.
- D5.3 is the requirement that bites: a replica placed in the same pass as its primary must **wait**, not fail.
- **Done when:** an index with one replica goes green, and a test that places both copies in one pass does not
  produce a failed shard.

**Confirmed end to end.** Two nodes, a one-shard segment-replication index with one replica: the supervisor
placed the primary on one and a `SEARCH_REPLICA` on the other, the replica's routing entry came out
`primary: false, searchOnly: true`, and a search against the *replica's* node returned a document written to the
primary's. That last step is the one worth having, and it is stronger than the milestone's stated "goes green":

- it proves the fabricated `RepositoriesMetadata` really does register the repository, which nothing but a
  running node could show — `RepositoriesService` could have rejected metadata it considers a system
  repository's, and the copy would have had nothing to open;
- it proves **both nodes derived the same remote path independently**. That is the subtlest requirement here,
  and had they disagreed the symptom would have been a perfectly healthy-looking replica returning zero hits;
- it proves the search-only marker holds under `-ea`: the replica applied a state whose routing table has no
  primary for that shard without tripping `RoutingNodes`, and strict search-replica routing then chose it.

What is *not* observed is D5.3's same-pass case — the primary had been serving for a long time before the
replica was placed. The argument that it is safe is the server's code rather than a run; see below.

**This milestone is really about the remote store, which no earlier milestone said.** The server picks a
replica's segment source in `SegmentReplicationSourceFactory`:

```java
if (shard.indexSettings().isAssignedOnRemoteNode()) return new RemoteStoreReplicationSource(shard);
else return new PrimaryShardReplicationSource(..., getPrimaryNode(shard.shardId()));
```

and `getPrimaryNode` resolves the primary through `clusterService.state().routingTable()` and then
`state().nodes()`. A clusterless node's fabricated state holds only its own copies and only itself, so on the
`else` branch both lookups come back empty. **Remote-store-backed segment replication is not one option of two
here; it is the only one that works before the peer resolution of M8/M9** — which is the scope this data plane was
given at the start, arrived at from the opposite direction.

So M7 added `RemoteStore`: read from this node's own `node.attr.remote_store.*` attributes, contributing the
repository to register and the index settings that point a shard at it. Three things about it are load-bearing:

- **`isAssignedOnRemoteNode()` comes from node settings, not index settings and not cluster state.** The
  repository is local operator configuration, like the store URI. The control plane says "segment replication" and
  the node says where its segments live, so nothing new is needed in the key layout and no tenant names an
  operator's bucket.
- **Nothing else would ever register the repository.** `RepositoriesService` builds repositories by applying
  cluster state, and the only main-code callers of `RemoteStoreNodeAttribute` are in `JoinTaskExecutor` — the
  cluster-manager's join path. A clusterless node never joins, so the fabricated `RepositoriesMetadata` is the
  whole mechanism. It arrives before the shards that need it because `RepositoriesService` registers its applier
  first, and both are high priority.
- **No remote-store custom data is stamped, deliberately.** `determineRemoteStorePathStrategy` reads the index
  metadata's `REMOTE_STORE_CUSTOM_KEY` and falls back to `PathType.FIXED`. One cluster manager stamping that once
  is fine; every node stamping its own is not, because a primary and its replica would then be free to disagree
  about where in the repository their segments live — and the symptom of disagreeing is a replica that reads an
  empty store rather than an error.

**D5.3 turned out to be satisfied by the server's own path**, which is the best available answer. A search replica
recovers through `recoverFromStore` → `internalRecoverFromStoreForSearchReplica` → `completeRecovery`, which
creates an empty store and *then* opens the engine with `syncFromRemote=true`. A replica placed in the same pass as
its primary therefore starts empty and catches up through segment replication instead of failing. Its recovery
source is an ordinary `EMPTY_STORE` or `EXISTING_STORE` — asserted as such inside that method — so
`LocalShardStore`'s existing decision needed no change at all.

**Two index settings, two different reasons, and they are not the same condition.** This is the part worth
reading before touching `settingsFor`:

- `index.blocks.search_only` is set when the node holds **any** search replica of the index. `RoutingNodes` asserts
  `"Primary shard routing can't be null for non-search-only indices"` for any shard whose routing table has no
  primary, and `IndicesClusterStateService` builds `RoutingNodes` on every applied state, so without it a search
  replica trips an assertion on a server that runs with them enabled.
- `index.number_of_search_replicas` is set to 1 only when the node holds **nothing but** search replicas.
  `cluster.routing.search_replica.strict` defaults to true, and with it on `OperationRouting` injects
  `Preference.SEARCH_REPLICA` into every search that named no preference — for any index whose metadata says it has
  search replicas. On a node holding a primary that preference selects from an empty set, so a healthy primary
  would quietly answer nothing.

The first of those reads like it should cost writes and does not, which is what makes the mixed case workable —
and the mixed case is the *natural* placement, not an edge: two nodes and a two-shard index with one replica each
gives every node a primary of one shard and a search replica of the other. `INDEX_SEARCH_ONLY_BLOCK`
(`ClusterBlockLevel.WRITE`, 403, "index scaled down") is only ever derived from the setting by cluster-manager code
— `MetadataCreateIndexService`, `RestoreService`, the gateway's `ClusterStateUpdaters` — none of which runs here.
This state is fabricated directly and sets no blocks at all. **Depending on the absence of a code path is exactly
the kind of thing that stops being true quietly, so a test pins it:** a fabricated state must carry no write
block, and if that ever changes the build fails instead of the user's writes.

Two copies are declined rather than built, both with reasons that name what to do:

- a search replica on a node with no remote store — it would otherwise start, report itself started, and never
  receive a segment;
- a search replica of a document-replication index — `index.remote_store.enabled` is validated against
  `index.replication.type` when the index service is created, so there is no way to back it with the store the copy
  would need to read.

## M7.1. A remote-backed primary that lost its disk — **done, and confirmed by wiping one**

Turning the remote store on has made an existing gap dangerous instead of merely visible.

`LocalShardStore` chooses `EXISTING_STORE` when there are bytes on disk for a shard and `EMPTY_STORE` when there
are not, and that was right while the disk was the only copy: a node whose data directory had been wiped
rebuilt an empty shard, which is honest and obvious. With a remote store it is neither. `StoreRecovery`'s
`internalRecoverFromStore` never consults the remote store — `indexShouldExists` is simply false for
`EMPTY_STORE` — and the remote path is a *separate recovery source*, `REMOTE_STORE`, which OpenSearch uses on
failover. So a primary that comes back without its disk starts empty **while a complete copy of the shard sits
in the repository, unread**, and it is then the authoritative primary of an index whose data it does not have.

Worse, it seals the fence on the way (see below), so it is authoritative in a way the previous writer cannot
contest. Nothing in the pass is wrong; the node simply answered a question it did not know it was being asked.

That was the state before this milestone, and it is worth keeping written down: the failure was silent and reached
the search replica, which faithfully syncs to whatever the primary uploads.

**Why it is not a one-line change.** The right source is `RemoteStoreRecoverySource`, but not unconditionally: a
brand-new index has nothing in the repository either, and OpenSearch gives *that* primary `EMPTY_STORE` on
purpose. So the node has to distinguish "new index" from "primary that lost its disk".

**And "err towards `REMOTE_STORE` when in doubt" — the first answer written here — is wrong.** It reads like the
safe direction and is not, because `REMOTE_STORE` recovery *fails* against an empty repository rather than
producing an empty shard. `RemoteFsTranslog.download` takes its `else` branch when there is no remote metadata
("we don't have any translog files uploaded to remote store") and creates no `translog.ckp`; back in
`recoverFromRemoteStore` the local store is then empty, so `listAll().length <= 1` holds and `Checkpoint.read(...)`
throws. Combined with this node's own retry behaviour — a failed copy gets a fresh routing entry on the next pass
— defaulting to `REMOTE_STORE` would leave a *new* remote-backed index failing its first recovery for ever. So
neither source is safe to guess:

| | remote store has data | remote store is empty |
|---|---|---|
| `EMPTY_STORE` | **silent data loss** | correct |
| `REMOTE_STORE` | correct | fails for ever, index never starts |

**Confirmed on a real pair of nodes, in the way that matters: both data directories were deleted.** A document
was written, both nodes were restarted without `--preserve-data`, and the primary logged a remote-store recovery
and returned the document — from both its own node and the search replica's. Before this milestone that exact
action destroyed the document silently, primary first and then the replica syncing to the empty state it
published.

What the run proves that no test here can:

- **the probe works against a real repository** — the factory call, the cast, `init()` returning non-null;
- **the path it derives is the path the shard uses.** This was the identified risk, and it is in the dangerous
  direction: `PathType.FIXED` plus the prefix setting had to resolve where the shard actually wrote, and a
  mismatch would have read an empty container, answered `EMPTY`, and discarded the shard while looking exactly
  like a healthy new index;
- **a search replica needs no probe.** Its directory was wiped too and it recovered through the ordinary store
  path — empty store, then sync from remote — which is why replicas are deliberately excluded from the lookup.

**And the other branch, on the same pair of nodes and the same repository.** A newly created index probed, was
told the repository held nothing for its shard, and started from an empty store — primary on one node, search
replica on the other, with the roles falling the opposite way round from the first index. So the discriminating
pair is covered: one shard that had been uploaded and one that had not, against a single live repository, told
apart correctly and given different recovery sources. That is the property the design rests on, and the only way
to test it is with a repository that really has one and not the other.

The `UNKNOWN` branch — decline with a reason — has only ever run under test, and exercising it for real needs a
deliberately unreachable repository. Left alone.

**Done, as `RemoteRecovery` plus `RemoteShardData`.** The answer is looked up rather than defaulted, and where it
cannot be looked up the copy is **declined for the pass with a reason** — the pattern `DesiredStateResolver`
already uses. A copy declined for a pass costs a pass; a copy built on a guess costs the shard or loops for ever.

Four things about the shape, each of which was a mistake avoided rather than a preference:

- **The probe runs in the poller, not the fabricator.** It reads a repository over a network, and the fabricator is
  a pure function called twice on an applying pass — the mistake this repository has made four times. It runs once
  per pass and only for copies about to be created *fresh*: a copy already in the live state keeps its routing
  entry and is never asked about, so a converged node probes nothing at all. That matters when the alternative is
  a network round trip per shard per second.
- **It goes through `RemoteSegmentStoreDirectoryFactory` rather than listing blobs.** Reproducing the path
  derivation would be the same hazard as two nodes disagreeing about it, with the worst possible failure: a probe
  looking in the wrong place finds nothing, and "nothing" is the answer that discards data. The two inputs still
  passed by hand — `PathType.FIXED` and the `cluster.remote_store.segments.path.prefix` setting — are pinned by a
  test asserting the fabrication stamps no remote-store custom data, since that is what makes `FIXED` correct.
- **Local bytes still win.** The looked-up source is consulted only where the fabricator would otherwise choose an
  empty store, so a disk that gained bytes since the lookup is believed over the lookup. The two cannot disagree in
  the direction that loses data.
- **`UNKNOWN` is not `EMPTY`.** An unreachable repository and an empty one are the same silence and opposite facts.

**Segments without a remote translog is a supported configuration, and it cannot use this path.**
`REMOTE_STORE` recovery restores through the remote translog — `syncTranslogFilesFromRemoteTranslog` asserts its
factory is the remote one, and that factory is chosen by `isRemoteTranslogStoreEnabled()`, true exactly when
`index.remote_store.translog.repository` is set. The upstream switch is
`node.attr.remote_store.mode=segments_only`, added for this architecture, which makes `RemoteStoreNodeAttribute`
require the segment repository alone. It is the right configuration when the ingestion source holds every
operation — pull-based ingestion makes a transaction log a second copy of something already durable, which is why
`cluster-etcd` disabled it — and a primary of such an index that has lost its disk is therefore **declined with a
reason** rather than started empty over its own data. Recovering *that* shard means segments plus an ingestion
checkpoint, which is a different mechanism and belongs with pull-based ingestion.

That mode also exposed a trap worth keeping: **`DiscoveryNode.isRemoteStoreNode()` is the wrong predicate to gate
on.** It requires a cluster-state repository, which `segments_only` does not have, while
`IndexSettings.assignedOnRemoteNode` comes from `isRemoteDataAttributePresent`, which such a node satisfies.
Gating on the first would have produced the one genuinely broken state available here: the server routing replicas
to `RemoteStoreReplicationSource` while this plugin had registered no repository and backed no index. `RemoteStore`
keys on the segment repository instead, and a test pins the distinction.

Note the asymmetry `LocalShardStore` already documents, one level out — and the third time the same rule has
decided a design here: **when the choice is between failing loudly and starting empty, fail loudly.** What was new
is that neither available recovery source fails loudly by itself, so the loudness had to be built.

## M8. Coordinators — **done, and confirmed by a shard fan-out**

- Route from `tenant/<t>/status/indices/<uuid>`, resolve addresses from `tenant/<t>/status/nodes/<id>`.
- **Done when:** a search against a coordinator that holds no shards returns results from data nodes.

This is the milestone that makes the system *usable* rather than merely converged, and it is the definition of
"complete" the supervisor's task model deliberately does not yet use — worth noting when you get here.

**It needed a contract change before a line of it could work.** `DiscoveryNode.equals` compares *only* the
ephemeral id, and `TransportService.connectionValidator` handshakes a new connection and then refuses it unless the
node it was given equals the node the remote reports itself to be. So `address` and `port` — all the node
projection carried — are necessary and not sufficient, and no reader can work around it: a fabricated ephemeral id
is unequal to the real one by construction. The supervisor now projects `ephemeral_id`, and
`NODE_CONTRACT.md` finding 8 records that the section promising addresses from that document was true about the
address and wrong about the outcome. The same wall was waiting for M9's docrep peer resolution.

**`CoordinatorState` is a decorator over `DataNodeState`**, because a node has one cluster state. A node that both
holds copies of an index and coordinates for its tenant needs a routing table containing its own copies *and* the
others' — which is what a real cluster state looks like, where two states would be neither. It also keeps the two
roles uncoupled for when coordinators move to dedicated capacity, which is where they belong: their traffic and
hardware are not a data node's. A pure coordinator is then the same decorator over a data path that was given
nothing to hold.

Four things in it are load-bearing, and three were found by a test rather than by thinking:

- **This node's own routing entries are copied across untouched.** The projection reports them too, a pass behind,
  and rebuilding one restarts a serving shard (D2.2). Copies the projection places *on this node* are skipped
  outright: the assignment is the authority for what this node holds.
- **Other nodes' entries are carried forward when node, role and state are unchanged.** Not efficiency:
  `initialize()` mints an allocation id, so rebuilding them every pass would make the routing table differ from the
  last one for ever and publish a state every poll interval. That is M5's bug in a new place, and
  `StateEquivalence` cannot see through it.
- **The coordinator owns the metadata of an index it holds no copy of, and rebuilds it each pass.** Leaving it
  alone looked right and was not: `DataNodeState` *retains* the metadata of an index it holds nothing of — that is
  what stops a closed index being deleted — so a settings change would have reached the routing view never.
- **`index.number_of_search_replicas` describes the fleet here**, where `DataNodeState` sets the local view. The
  rule is the same in both — the count must describe the routing table in the same state — and the number is
  opposite, because the tables are.

`DiscoveryNodes.Builder` refuses two node ids claiming one address — host *and* port, so co-located nodes on
different ports are fine, and the case that reaches it is a stale projection: a node whose data directory was wiped
returns with a new id on the same port, and until the old projection is reaped two ids claim one endpoint. One peer
that cannot be represented costs that peer's shards rather than every index in the state.

**Confirmed on two nodes, by a profiled search across two indices.** Each shard request was served by a different
node — one by the node holding that index's primary, the other by the node holding the other index's search
replica — with `total: 2, successful: 2, failed: 0`. That is the whole path: projections read, peers built from
`ephemeral_id`, the transport handshake accepted them, and shard-level requests came back. `_cluster/state` on a
data node shows both nodes in `nodes`, both copies per index in the routing table, and the repository registered
from node attributes.

**Two defects it exposed, both found by reading the output rather than by a failure.**

The first was mine: `index.number_of_search_replicas` said `0` beside a routing table containing a started search
replica, because `DataNodeState` sets that count from the local view and this class had left it alone. So
`cluster.routing.search_replica.strict` had nothing to act on and searches were spread across primaries and search
replicas rather than preferring the replicas — which is what search replicas are for. The rule was already written
down; it just was not applied to the state that actually ships.

The second is worth more than the milestone. **The search-only marker disables the timer that keeps a search
replica current.** `IndexService.AsyncReplicationTask` calls `maybeSyncSegments` for active search-only shards on
the refresh interval, and `shouldRun()` is `false == INDEX_BLOCKS_SEARCH_ONLY`. Upstream is right about that — an
index scaled down to read-only has no writer to follow — and the flag means something else here: "this node cannot
see the primary". So on a node holding nothing but search replicas, replication fell back to checkpoint publishing
from the primary, and `ReplicationTracker`'s replication group comes from the *primary's own* routing table, which
in a clusterless deployment does not contain the replica. Hence the `UnavailableShardsException` in a real run:
`Not enough active copies to meet shard count of [DEFAULT] (have 0, needed DEFAULT). Timeout: [1m]`.

The two together mean a search-replica-only node was refreshed by nothing after its initial recovery sync, and the
M7 verification did not catch it because a wiped data directory recovers by syncing from the remote store at engine
open, which looks identical to replication working.

**Both fixes are confirmed by one run, from two directions.** A document written to the primary — no restart — was
returned by the search replica, which is ongoing replication working rather than a recovery sync doing it. And a
profiled search sent to *each* node was served by the search replica's node both times, which is the count fix
visible in behaviour: `number_of_search_replicas` is now 1 on the primary's node too, so strict routing injects the
search-replica preference. Before the fix that count was 0 and the search landed on whichever copy the default
iterator chose.

Worth knowing for next time: the `fs` repository logs `loading all blobs with prefix into memory for sorting` once
per listing, from `BlobContainer`'s default `listBlobsByPrefixInSortedOrder`, so a polling search replica produces
one WARN per refresh interval. That noise *is* the evidence replication is live — it is the polling the marker used
to disable. The run cluster quiets that one logger; remove the setting to watch it again.

The marker is now dropped as soon as the routing table has a primary for every shard — which coordinating
provides. **Which makes coordinating on a search-replica node worth more than routing:** it is what lets that
node's copies stay fresh without depending on a primary that has never heard of them. A pure data node holding
search replicas still has the problem, and that is the argument for resolving peers of one's *own* shards
independently of coordinating whole tenants — the mechanism M9 needs anyway, wanted here for freshness rather than
correctness.

## M8.1. Peers of a node's own shards — **done, and confirmed with nothing coordinating**

The loose end M8 exposed, and it is not about routing. A data node that answers no queries still needs the other
copies of the shards it holds:

- a primary publishes segment-replication checkpoints to the replicas in **its own** routing table, so a primary
  that does not know its replicas publishes to nobody — seen as `UnavailableShardsException: Not enough active
  copies ... (have 0, needed DEFAULT). Timeout: [1m]` once per checkpoint;
- a search replica whose primary is missing from the table needs `index.blocks.search_only` to satisfy
  `RoutingNodes`, and that setting disables `IndexService.AsyncReplicationTask` — the timer that keeps it current.
  So a replica that cannot see its primary is refreshed by nothing at all.

Both are freshness rather than reach, and both were invisible until a coordinator happened to supply the missing
entries. Before this, the only way to keep a search replica current was to give its node the coordinator role,
which is a much larger statement than the problem needs.

**Same projections, narrower question.** A coordinated tenant contributes every shard of every index it has; an
index this node merely holds copies of contributes only the shards it holds, and needs no index-document read at
all, because its configuration was already read to build the copies. `CoordinatorView.RoutedIndex` carries the
shard set, so both cases go through one reader and one fabricator, differing only in what they ask for.

**Cost is bounded by tenants, not indices**, which is the part worth getting right. It is one
`scanPrefix(tenant/<t>/status/indices/)` per tenant this node has anything to do with, not one read per index — a
node holding a thousand indices would otherwise make a thousand reads per pass, which is the cost the contract
avoided by keeping this table out of the assignment in the first place.

**Done when:** a search replica on a node with no coordinator role stays current — a document written to the
primary appears in a search of that node's copy, with nothing coordinating anywhere.

**It does.** Two nodes, no coordinator role: a document written to the primary was returned by a search of the
search replica's node, and neither node's state carries `index.blocks.search_only` — which is what says the polling
timer is enabled. Both nodes report `peers: {indices: 2, serving_copies: 4}`.

Two readings of that run worth writing down, because both are easy to get backwards:

- **The absence of the `FsBlobContainer` noise proves nothing**, since the run cluster now sets that logger to
  `ERROR`. The evidence is the missing marker plus a document that arrived. Removing the logger setting is what
  turns the noise back into a signal.
- **`PublishCheckpointAction` still failed once, and it now means the opposite of what it used to.** In this run it
  fired on the primary's node *before the second node had started*, so there genuinely was no replica to publish to:
  transient, correct, and it resolves when the replica appears. Before M8.1 the same message meant the replica
  existed and the primary could not see it — permanent, and the reason search replicas were never refreshed. Same
  exception, opposite meaning, and only the timing distinguishes them.

### A saving not taken: coordinators do not need mappings

Noted rather than done, because it is not urgent and the tradeoff is real. `REQUIREMENTS.md` open question 7
carries the short version.

A node that holds no copy of an index needs its metadata to route to it — a shard count and the identity settings —
and it does **not** need its mappings. Nothing on the coordinator's search path reads them: `QueryRewriteContext`
carries no `MapperService`, and field resolution happens on the shard through `QueryShardContext`. `_field_caps` is
a shard-level action too. So `CoordinatorState.metadataFor` currently puts mappings in the fabricated metadata that
nothing on that node will ever consult, and for a coordinator that knows thousands of indices with large mappings
that is worth something.

Be accurate about how much: `MappingMetadata` holds a `CompressedXContent`, so the cost is the compressed source
per index rather than a parsed field tree — real at ten thousand indices, and not the order of magnitude a
`MapperService` would be.

What it costs to omit them: `GET <index>/_mapping` against a coordinator-only node would answer with an empty
mapping. That request is a `ClusterManagerNodeReadRequest`, so `ClusterManagerActionFilter` answers it from local
state — meaning the same call would return the mapping from a data node holding a copy and nothing from a
coordinator, which is the kind of inconsistency that costs somebody an afternoon. Worth doing with a deliberate
answer to that, rather than as a one-line saving.

The change itself is one line in `metadataFor`, guarded on nothing: that method is only reached for an index this
node holds no copy of, which is exactly the case where mappings are unused.

## M8.2. Pull-based ingestion — **done, and the pause now has somewhere to live**

`index.ingestion_source.*` is entirely index settings, which means the data plane needed **no code to support it**:
the control plane sets them, the fabrication passes them through, and `IndicesService.getEngineFactory` branches on
`indexMetadata.useIngestionSource()` to build an `IngestionEngineFactory`. No cluster manager is involved anywhere
in that path. The node does need the source's plugin installed, and the failure without it is at least legible —
`No factory found for ingestion source type [FILE]`.

**What did need changing is recovery, and it closes M7.1's remaining hole.** `IngestionEngine` uses a
`NoOpTranslogManager`: a pull-based index has no translog at all, local or remote. So remote-store recovery — which
restores segments *and a translog* — has nothing to offer it, and M7.1's decline for "no
`index.remote_store.translog.repository`" was the wrong answer for exactly the configuration `segments_only` exists
to serve.

The right answer is in the commit. `IngestionEngine` reads `StreamPoller.BATCH_START` out of the Lucene commit user
data and resumes from it, falling back to `index.ingestion_source.pointer.init.reset` when there is no commit. So:

- **local bytes** — the commit carries the pointer, so resuming from disk resumes from where this copy left off.
  Cheapest and most correct, which is why it is still checked first;
- **no local bytes** — start empty and let the poller replay. `earliest` is correct at the cost of doing all the
  work again; `latest` starts at the head and whatever arrived while the shard was gone is not indexed by this copy.
  Both are the tenant's declared policy rather than this node's decision, and the log line says which one is about
  to happen, because "this shard is very busy" and "this shard is short of documents" are otherwise hard to explain.

A pull-based index is therefore **never probed** — nothing the repository could say would change the answer — which
also removes a network read per fresh copy.

**Correcting the first bullet, which said "resumes from exactly where this copy left off".** It does not, and the
word "exactly" was wrong in a way that matters. `innerOpenEngineAndTranslog` calls
`syncSegmentsFromRemoteSegmentStore` on **every** open of a remote-store-enabled index — the `EXISTING_STORE` path
included, because `internalRecoverFromStore` uses the no-arg `openEngineAndRecoverFromTranslog()` and that passes
`syncFromRemote = true` — and that method **deletes every local `segments_N` and commits the downloaded infos in
its place** (`IndexShard.java:6278-6295`). So the `BATCH_START` the poller resumes from is the **last uploaded**
commit's, not the last local one. Identical on a shard that refreshes and uploads constantly; different on an idle
or paused one, which is exactly where a resume gets watched. `WORKING_NOTES.md` has the mechanism.

**And it can fail outright, which is how this was found.** A restart with a surviving data directory produced
`TranslogCorruptedException[... expected shard UUID ETNX... but got: oSd_8... this translog file belongs to a
different translog]` from an index that has no translog at all. The chain is upstream's and it is recorded in
`WORKING_NOTES.md`: `InternalEngine.<init>` calls `trimUnsafeCommits` unconditionally, so an `IngestionEngine` with
a `NoOpTranslogManager` still validates a translog file against the commit's `TRANSLOG_UUID`; the commit has just
been replaced by the downloaded one, whose UUID belongs to whichever copy uploaded it; and the fix-up that exists
for precisely this — recreating the local translog with the downloaded UUID, `IndexShard.java:3404-3421` — is in the
**replica** branch. A primary takes the other branch, which acts only when `isRemoteTranslogStoreEnabled()`, and
that is false in `segments_only`. So a `segments_only` primary falls through both branches with nothing rewriting
its translog.

**That is an upstream gap rather than something this plugin can work around**, and the reason is worth stating: the
download happens inside `innerOpenEngineAndTranslog`, after the recovery source has been chosen and before the
engine exists, so no choice available to `RemoteRecovery` sits on either side of it. `recoverFromRemoteStore` calls
the same no-arg open, so `REMOTE_STORE` is not an escape either.

### The gap, now closed: pausing ingestion had desired state with nowhere to live

`IndexMetadata.ingestionStatus` is a **first-class metadata field, not a setting** — `IngestionStatus(isPaused)`,
defaulting to not-paused, changed upstream by `MetadataStreamingIngestionStateService` through a cluster-manager
action. In this architecture that action is refused, correctly: pausing an index's ingestion is desired state and
belongs to the control plane.

But `tenant/<t>/indices/<name>` had no field for it, and it cannot be a setting, so **a paused index came back
unpaused** — every fabrication defaulted it, and the pass after an operator paused would resume the stream
underneath them. That was raised with the supervisor as finding 6 rather than patched here, because a data plane
inventing a place for it would be the two services disagreeing about desired state, which is the one thing this
design cannot tolerate.

**Both other services have since added it**, and the adopted spelling is upstream's rather than the one this side
sketched: `ingestion_status: { is_paused: true }` at the top level of the index document, matching
`IndexMetadata`'s own key and `IngestionStatus.IS_PAUSED`. Worth noting that the sketch here proposed
`ingestion: { paused: true }` and the better answer was to match the server — a reader of any of the three
repositories now finds the same two words.

This side is three lines and no new plumbing, which is worth explaining because it is the level-triggered design
paying off. The chain, read from the server's source rather than watched on a node:

- `IndexConfig` reads the field, absent meaning not paused — which is what makes every index document written
  before the pause existed still legal;
- both fabricators put it into the metadata as `IngestionStatus`, unconditionally. That is only safe because
  `IngestionStatus(false)` is exactly what `IndexMetadata.Builder.build()` would have defaulted to, so an ordinary
  index fabricates byte-identically and no deployment gains a metadata change on the pass this shipped;
- `StateEquivalence.sameContent` compares `IndexMetadata` by `equals`, which includes `ingestionStatus`, so
  flipping the pause is a change and therefore publishes;
- `IndicesClusterStateService.createOrUpdateShards` calls `updateShard` for every shard on this node on **every**
  applied state, with no equality guard on the routing entry, and that calls
  `IndexShard.updateShardIngestionState(IndexMetadata)` → `IngestionEngine.updateIngestionSettings`. So a
  metadata-only change with an identical routing table reaches a *running* poller;
- and `IngestionEngine` reads the same field at open for `initialPollerState`, so a copy built while paused starts
  paused.

Both directions, no latch, and the test that matters is the one asserting the flip is visible to
`StateEquivalence` — a pause that comparison cannot see would be written, read, fabricated, and never applied to
anything.

**Confirmed on a running node.** Pausing and resuming an index's ingestion both work against a real three-service
deployment — the chain above is no longer only a reading of the server's source. Note what the run needed first:
the restart it took to get there is what surfaced the `TranslogCorruptedException` recorded below, so "the pause
works" and "a pull-based primary can restart" were separate facts that had to be established in that order.

**And the resume catches up from the pointer**, which was the last thing here that a document count could have
contradicted. See the reset section below for the run that showed it, and for the trick that made it visible at
all.

### Consumer-pointer resets — **done on both sides, and confirmed on a running node**

`_resume` may carry `reset_settings` — per shard, move the consumer to an offset or timestamp. Those are
instructions rather than desired state, so the control plane puts them on the task and the supervisor carries them
into this node's assignment as `ingestion_reset`, tagged with the task id. The design is `DESIGN.md` §21.2–§21.6
and the obligations on this side are in `NODE_CONTRACT.md`'s assignment section, which is where they belong: **a
node that trusted the ordering would pass every test in which the reset happened to arrive first.**

Why an acknowledgement rather than reading the pointer back, since it is the part worth not re-deriving: a shard
reset to offset 1000 starts advancing immediately, so a position read afterwards cannot distinguish a reset that
worked from one that never happened — and worse, a rewind from 5000 back to 1000 looks, *before* it is applied,
exactly like a job already finished. Any completion rule that reads the pointer drops that instruction before
anything acts on it.

**The sequencing was wrong in the first version of the design, and this side found it.** Upstream's
`IngestionEngine.resetStreamPoller` throws on a poller that is not paused, and the control plane writes
`is_paused: false` *before* it records the task — so the two halves of one operator action arrive down paths of very
different lengths. The resume is desired state in a document already read every pass; the reset is an instruction
that must be observed, planned, written and read. The fast path is the wrong one, so **the ordinary case is a
poller that has already resumed**. Not a race to narrow: the normal ordering. Raised as finding 8, accepted, and
`DESIGN.md` §21.3 now records it as a general hazard of mixing state with instructions about the same object.

What is implemented here:

- `IngestionReset` reads the instruction, keeping an unrecognised **mode** rather than dropping it — the opposite
  of what an unknown shard role gets, because an unbuilt copy is visible to the supervisor's differ and an
  unapplied reset is visible only if this node says so;
- `IngestionResets.plan` is a pure function of (assignment, cluster state, what has already been applied), so
  every rule is testable without a running shard; `IndicesServiceIngestionPollers` is the only part that touches
  one;
- **two calls, and the order is the point.** Pause the poller whatever it was doing, then reset and land on
  whatever `ingestion_status.is_paused` asks for. One call cannot do it: upstream applies the reset *before* the
  pause flag within a single call, so the reset would still run against a live poller;
- **`applied` means the reset too.** An outstanding one holds the ack at `applying`, a refused one makes it
  `failed`. Without that the supervisor completes the task on this node's report and tells an operator a pointer
  moved when nothing moved it — in the one case where nothing downstream can ever contradict the claim;
- **idempotence keys on the reset id**, never on the assignment version, which moves whenever anything else in the
  document does. A test pins that, because re-applying a reset because an unrelated field changed is the
  rewind-for-ever failure arriving by another route.

Nothing needs a durable ledger, and §21.4 is why: `resetStreamPoller` force-flushes the new start pointer into the
Lucene commit, and M8.2's recovery already prefers local bytes and reads `StreamPoller.BATCH_START` out of the
commit's user data. A copy that moves after a reset resumes from the reset point with the instruction existing
nowhere at all.

**Two holes, both real rather than theoretical.**

**The landing state was read from the wrong place, and a running node found it.** This plugin logged "left
ingestion paused" while carrying out a reset that arrived with a resume. Applying a cluster state is asynchronous
-- `ClusterApplierServiceStateApplier` queues it and returns -- so on the pass an operator's resume arrives, the
applied state still says paused while the index document says running, and the reset landed the poller on the
stale value. It is not self-correcting: the pass after fabricates an identical state, `StateEquivalence` sees no
change, nothing is published, and `updateShardIngestionState` is never called again, so the stream stays stopped
for ever by the instruction meant to restart it. The pause state now comes from the `IndexConfig` this pass read,
which is the only authority that cannot be stale, and a test pins the two disagreeing.

**The applied-id memory does not survive a restart.** It lives in this process, so a restart
between applying a reset and the supervisor observing the acknowledgement re-applies it, rewinding the consumer by
whatever it consumed in between.

**Correcting what an earlier version of this said:** "bounded by one supervisor pass" was an understatement. The
supervisor has since measured the hand-over/acknowledge/withdraw cycle at **about three of its passes**, because the
reset is withdrawn only once the acknowledgement has been observed — so the exposed interval is the part of that
cycle after this node applies, not one pass. A test now walks four passes with the reset still present rather than
two, which is the difference between exercising that window and clipping it.

It still re-consumes rather than loses: every ingested message must carry `_id` — `MessageProcessorRunnable` rejects
one that does not — so a replay re-indexes the same documents rather than duplicating them. The exception worth
knowing is a stream of `_op_type: create` messages, where a replay is a version conflict per document and the error
policy decides whether that drops or blocks.

**Left open deliberately, and the reason is worth keeping**, because the obvious fixes are both worse:

- **The pointer cannot be used to detect that a reset happened.** A rewind from 5000 to 1000 looks, before it is
  applied, exactly like a job already finished — which is the whole reason the design uses an acknowledgement. So
  "is the consumer already at or past the target" would drop the instruction rather than protect it.
- **The only durable place that would work is the Lucene commit**, beside `StreamPoller.BATCH_START` where the
  reset's own effect already lands. That is the engine's to write and not a plugin's, so closing this properly is an
  upstream change rather than a local one — and a ledger anywhere else would have to be correct across exactly the
  crash it exists for.

**Confirmed end to end, and the way it was confirmed is the part to remember.** A reset is very nearly
unobservable: because every ingested message carries its own `_id`, re-consuming a range rewrites the same
documents with the same content, so a reset that worked and a reset that did nothing produce an identical index.
A document count does not separate them either — the count is unchanged either way, which is what the earlier
note here asked for and would not have got.

The run that settled it changed the *payloads* behind the stable ids: the ndjson source was edited so the three
documents read `foo modified`, `bar modified`, `baz modified`, then the consumer was reset to offset 2. A search
afterwards returned `foo`, `bar`, `baz modified` — only the third document had been re-indexed. That single result
pins three separate things: the pointer landed at exactly offset 2 (**offsets are 0-based line numbers for the
file source**, so offset 2 is the third line), the poller was running afterwards rather than left paused, and it
consumed from the pointer rather than replaying the file. `DefaultStreamPoller` logged
`Resetting pointer by seeking to pointer 2` in the same run.

**Worth knowing that the technique does not transfer.** Editing history is a property of a file source; Kafka
does not let a producer go back and change an earlier message, so on the deployment this is actually built for
there is no way to make a replay visible from the index alone. Anything that needs to *observe* a reset in
production has to watch the consumer position as it happens, not the documents afterwards.

**Which cuts the other way on the restart hole above.** The same id-stability that makes a replay harmless makes
it undetectable: a node that re-applies a reset after a restart rewrites the same documents and leaves no trace in
the index at all. The bound on that hole is real, but nobody will ever notice it being hit.

**And it now has a test, which needed an assumption to be wrong.** Every "what is not proven" note about this seam
said a started primary with an `IngestionEngine` behind it could not exist in this sandbox. That was true of
`tools/jbuild.sh` and false of Gradle: `OpenSearchSingleNodeTestCase` starts a real node here.
`IndicesServiceIngestionPollersTests` uses one, with an in-test `ListIngestionSource` whose payloads can be
rewritten behind stable ids — the operator's trick, since that is the only way a rewind is visible. It asserts a
reset on a *running* poller succeeds, lands running, and re-consumes from exactly the offset given and no earlier.

**Removing the pause-first call fails it with upstream's own message** —
`IllegalStateException: Cannot reset consumer when poller is not paused`. §21.3's argument was read from the
server's source; it is now executed.

**One upstream behaviour the test found, and it is worth knowing before an operator meets it.** A reset always
brings the consumer up *running* for a moment, whatever pause state it is landing on: `resetStreamPoller` ends with
`streamPoller.start()`, and `updateIngestionSettings` applies the pause flag only afterwards. So "reset while
staying paused" still ingests whatever is available at the new pointer. The first version of that test asserted
nothing would be consumed and failed for exactly this reason; it now asserts the thing that is true and matters —
that a poller left paused does not consume what is published *after* the reset.

## M9. Document replication

- Peer resolution, primary term handling, push-based ingestion.
- **Done when:** an index with `index.replication.type: DOCUMENT` indexes and replicates.

Expect D8.2 to be in the way. Read it before starting, and treat what you learn as a finding.

**What was learned, before starting.** D8.2 asks for a mechanism that would let a superseded primary be fenced.
One exists and **this deployment cannot use it**: the remote store's fence is a CAS token claimed on the *translog*
repository, which `segments_only` does not have, and both of its call sites are gated on
`isRemoteTranslogStoreEnabled()`. `WORKING_NOTES.md` has the mechanism and the line numbers, and corrects an
earlier claim there that the fence improved D8.2's outlook — it would, if it could be claimed. The enforcement side
already reads fence ownership on the **segment** path, so this is one repository-shaped gap in OpenSearch away from
being usable, which is the finding worth carrying upstream.

**What that means for this milestone specifically, and it is narrower than it sounds.** A document-replication
index is not backed by the remote store here at all — `RemoteStore` keys on the segment repository and a docrep
index does not get one — so M9's exposure is not D8.2's shared-remote-path corruption. It is two primaries
diverging *locally*, which is a different failure with a different answer, and the mitigation for both remains the
same: the supervisor never moves or fails over a primary.

**The deferral reason in `DesiredStateResolver` is stale** and should be the first thing fixed: it declines a
`REPLICA` saying peer resolution is not implemented, and M8.1 implemented it.

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
