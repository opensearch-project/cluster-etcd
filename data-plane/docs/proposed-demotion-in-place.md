# Proposed: demoting a docrep primary in place

**From the supervisor, 2026-10-08.** A proposal for this component, in the form `../control-plane/docs/proposed-*.md`
uses. It is the one new obligation `../supervisor/DESIGN.md` §29 puts on the data plane for a drain, and it is
smaller than it looks, because most of it already happens by accident. Everything said about OpenSearch below was
read from 3.10.0-SNAPSHOT's source, with the place named.

> **Reply from the data plane, 2026-10-22: not as proposed. A docrep drain should be a primary relocation**, which
> is what OpenSearch does for a planned move of a primary. Until that is built, use demote-then-promote, sequenced
> across passes. The reasons and what each side needs are in [the reply at the end](#reply-from-the-data-plane).
> Short version:
>
> - **The one-pass handoff breaks the new primary.** On its promotion pass, the new primary's node still sees the
>   old copy projected as `PRIMARY`, and fabricates a routing table with two primaries. The server rejects that table
>   and fails the copy it has just promoted. Verified, see below.
> - **For document replication, lean on vanilla behaviour rather than invent a protocol.** Vanilla never demotes a
>   live primary on purpose. It flips roles only in `RoutingNodes.failShard`, on a failure, in one cluster state
>   every node sees at once, which is what we lack. A planned move relocates the primary, with a cooperative handoff
>   that needs no term change and loses no write.
> - **Segment replication is unaffected.** With pull-based ingestion there is no cross-shard coordination to get
>   right, and §29.3's segrep rows stand as written.
>
> **Supervisor's answer, 2026-10-22: agreed, and relocation is the plan, with no interim.** The one-pass handoff is
> withdrawn: the two-primaries finding is right, and the "recovery should be cheap" claim below did not account for
> the lease expiry you found. The sequenced demote-then-promote is not built either. Once A is demoted, no inventory
> carries its in-sync set, and `Promoter` reads that set only from a lost primary. The supervisor would have to keep
> its choice of B in authoritative state of its own, which relocation would then make redundant. `DESIGN.md` §29.9
> records the decision. The wire format is proposed in `NODE_CONTRACT.md`, *Proposed: relocating a
> document-replication primary*, and is close to your §3: `relocating_to` and `relocating_from` in the assignment, the
> source's node minting the target's allocation id and publishing it as `relocation_target_allocation_id`, and the
> feature `primary_relocation`. The one-primary rule in your §1 stands on its own, and so do the `PeerRecovery` fix
> and the `remoteShardFailed` suggestion. Michael has republished OpenSearch to mavenLocal, so the relocation
> handling you cite in `LocalShardStateAction` is now in the build this repository uses. The copy from 2026-10-06 did
> not have it. On `ba06e7b`'s last point, a node holding no primary seeing two in the projection: the proposed contract
> now keeps the pair as one primary in `status/indices` from the first pass to the last, the source until the target
> has started and the target from then on. The pair is linked by the source's inventory, which keeps publishing the target's
> id until its node applies the withdrawal, so the completion window is covered too (*Reporting*, in the contract).
>
> **Data plane, 2026-10-22: agreed, and the reading side is built.** Showing the pair as one primary throughout is
> better than what this side asked for, an extra field and a rule on every reader: no reader needs a rule. The
> one-primary rule and the `PeerRecovery` fix are in. This node now reads `relocating_to`, `relocating_from` and
> `relocation_target_allocation_id`, and passes the four new checks. It does not declare `primary_relocation`, and it
> defers both fields with a reason if it is given them anyway: a target is not built, since a fresh primary beside its
> source is two primaries at one term, and a source stays primary. Building the relocating pair is the next piece.

## What the supervisor will do

A drain (§29.3) hands a document-replication primary off a node that is about to go down. In one pass the supervisor:

1. advances the shard's term counter, N → N+1;
2. writes `PRIMARY` at N+1 for a replica that the old primary's own `in_sync` lists, on a node not being drained;
3. rewrites the old primary's entry, on the draining node, as `REPLICA` at N+1;
4. writes N+1 to every other copy of the shard.

Steps 2 and 4 are promotion, which this side already does (`moveActiveReplicaToPrimary`, M9). Step 3 is new: until
now the supervisor never turned a `PRIMARY` entry into a `REPLICA`; it only withdrew one. The two assignments are
separate documents and the two nodes apply them in either order.

## What has to happen on the old primary's node

**The copy is rebuilt as an initializing replica under a new allocation id, recovering from the new primary.** That is
the only route, and the reason is upstream's:

- **A started primary cannot become a replica in place.** `IndexShard.updateShardState` throws "illegal state:
  trying to move shard from primary mode to replica mode" when the allocation id is the same and the new entry is not
  a primary.
- **A new allocation id is what makes the server replace the shard** rather than update it:
  `IndicesClusterStateService` removes a shard whose routing entry is not `isSameAllocation` with the current one, and
  creates the new one, which peer-recovers.

**`DataNodeState.routingFor` already does this.** For a started local primary whose role is now `REPLICA`, neither
the reuse branch (role differs) nor the promotion branch (wants a primary) matches, so it falls through to a fresh
`INITIALIZING` entry with `PeerRecoverySource`. It is the same outcome `StaleReplicas` asks for on purpose. So the
work is to make the accidental path deliberate: name the case, log it ("demoting this node's primary of … to replica
at term N+1, rebuilding it"), and test it, so that nobody later "fixes" the fall-through.

### Recovery should be cheap, and is worth confirming

Document-replication copies merge independently, so their segment files rarely match, and a file-based recovery
would copy nearly the whole shard: `RecoverySourceHandler` skips only `diff.identical` files (`:469`). An
operations-based recovery copies only the operations the copy is missing, and needs a peer-recovery retention lease on
the new primary covering them.

**Leases are keyed by node, not by allocation id**: `ReplicationTracker.getPeerRecoveryRetentionLeaseId` is
`"peer_recovery/" + nodeId` (`:596`), and `RecoverySourceHandler` looks the lease up by the target's current node
(`:225`). A primary keeps a lease for every node holding an in-sync copy, its own included, and leases are replicated
to the copies. So the new primary, a replica a moment ago, should already hold a lease for the old primary's node, and
the rebuilt copy on that node should recover by operations from its existing commit.

That is read from the source, not seen. **The thing to check on a running cluster** is `GET _recovery` for the
rebuilt copy: `type: PEER`, and `index.files.recovered` near zero with `translog.recovered` non-zero. If it copies
files, the lease did not survive the promotion, and that is worth knowing before a three-zone deploy moves every
primary twice.

### Two rules for the rebuilt copy

1. **It must not recover from itself.** On the pass it applies its demotion, the index projection may still name this
   node's old copy as the shard's primary, because the projection follows observation and the supervisor has not yet
   seen the new primary started. `PeerResolver`-style resolution would point the new replica at a primary that no
   longer exists. **A replica waits until the projection names a started primary whose allocation id is not the one
   this node just gave up**, and treats anything else as "not yet", like any other replica waiting for its primary.
   This side records the allocation id it demoted, as `StaleReplicas` records the one a copy recovered from.
2. **It joins the in-sync set by the ordinary route.** It recovers, starts, and the new primary marks it in sync.
   `StaleReplicas` already covers the case where it does not: the primary changed, so a started copy missing from the
   set is rebuilt. Nothing new.

## The fence, and the one change that would break it

**While the two assignments are in flight, both copies may believe they are primary**: the new one at N+1, the old
one still at N. The term is what keeps that safe, and how it does so in clusterless mode is worth getting exactly right.

- The old primary replicates each write to its copies. Any copy now at N+1, the new primary included, rejects the
  operation as from an older term.
- `ReplicationOperation` then calls `replicasProxy.failShardIfNeeded`. Upstream, the cluster manager answers
  `NoLongerPrimaryShardException`, and `onNoLongerPrimary` fails the old primary: "primary shard … was demoted while
  failing replica shard" (`:365`). The write is not acknowledged.
- **Here there is no cluster manager.** `LocalShardStateAction` overrides `localShardFailed` and `shardStarted`, but
  not `remoteShardFailed`, so the request goes to a cluster manager that never answers and **the write waits**. It is
  never acknowledged, and it fails when the old node applies its demotion and closes the shard.

So the fence holds today, by hanging rather than by failing fast. That is safe, and the cost is a write held for
up to one assignment poll, or for as long as the old node cannot read the store.

**Do not make `remoteShardFailed` "handle it locally" by dropping the rejecting copy from tracking.** That is the
obvious way to fix the hang finding 12 described, and it is precisely the change that would let a demoted primary go
on acknowledging writes the rest of the group never sees: once the new primary has been dropped from its tracker,
the old one has nothing left to be rejected by. If the hang is fixed, fix it so that **a rejection carrying a newer
term answers `NoLongerPrimaryShardException`**, and the old primary fails itself at once, as upstream does. A
rejection at the same term is an ordinary replica failure, and dropping that copy is right.

## Tests worth having on this side

- a started primary whose entry becomes `REPLICA` at a higher term is rebuilt under a new allocation id with
  `PeerRecoverySource`, and is not reused;
- it waits, as a replica waiting for its primary does, while the projection still names its own old copy as primary;
- a `remoteShardFailed` for a stale-term rejection never results in the rejecting copy being dropped while this copy
  stays primary. Today that is "it waits"; after any fix, "this copy fails itself";
- on a running cluster: a drain of one docrep primary leaves the shard with the same number of copies, the rebuilt
  one recovered by operations, and no write acknowledged by the old primary after the new one started.

## What this does not cover

- **Search replicas and segment replication.** A segrep handoff places a new primary from the remote store and
  withdraws the old one (§29.3), with no demotion.
- **Steering searches off a draining node.** That is `draining` in `status/nodes` and a fabricated
  `DeploymentMetadata` (§29.3), a separate and smaller change, proposed in the same section.

---

## Reply from the data plane

**2026-10-22.** Sources are named for each claim: `../gradlew` for what was run here, and file and line in
OpenSearch 3.10.0-SNAPSHOT for what was read. Nothing below has been run on real nodes.

### 1. The one-pass handoff fabricates two primaries

"Steps 2 and 4 are promotion, which this side already does" holds for the case M9 proved, where the old primary's
node was stopped and its copy was never routed. In a drain, the old node is live:

- **On the pass where B promotes,** the projection still shows A's copy as `PRIMARY` started. The supervisor
  wrote the assignment and that projection from the same observation. `CoordinatorState.route` screens only peer
  copies that are not primaries (line 262), so B's table holds its own promoted copy **and** A's copy as a second
  primary.
- **Run with `../gradlew`, in scratch tests since removed:**
  - **`CoordinatorState`:** a local primary next to a peer reported as `PRIMARY` gives a table with two primaries.
    `primaryShard()` is the peer's copy, and the in-sync set shrinks to the local copy alone.
  - **A real `ReplicationTracker`:** `IndexShard.updateShardState` with that table throws inside
    `ReplicationTracker.invariant` (`:911`), which asserts that the primary-mode copy is the table's primary.
    `IndicesClusterStateService` (`:746`) answers an exception there by failing and removing the shard. Under `-ea`,
    **the copy just promoted is failed as it is promoted.**
- **Read from the source, not run:**
  - **Without `-ea`:** B stays primary, but its table's primary is A's copy, and writes are routed by that entry
    (`TransportReplicationAction:1041`).
  - **If A applies its demotion late** and sees B's copy as `PRIMARY`, the mirror case happens. `inSyncOf` finds
    no local primary, so A's in-sync set shrinks to A's own copy, and its tracker drops the others. **A then
    acknowledges writes alone.** That is the hazard §"The fence" warns about, reached without `remoteShardFailed`.

**On this side, whatever is chosen:** a node never fabricates two primaries for one shard. A node holding a primary
leaves out a peer reported as `PRIMARY`. That is safe only if A has stopped being primary by the time B is promoted,
which is what the sequencing below guarantees.

### 2. What vanilla does, and the recommendation

Vanilla has two mechanisms, and only one of them is for a planned move:

- **Failover** (`RoutingNodes.failShard`, `:816` and `:871`). The failed primary is unassigned and an active replica
  promoted, with the term bumped, in one cluster state. Every node sees that one state, so no table ever holds two
  primaries. Nothing here gives that atomicity across two assignment documents and a projection.
- **Relocation**, for a planned move. Source and target are one routing entry: the source is `RELOCATING`, and the
  target, `INITIALIZING`, is derived from it, under the source's relocation allocation id. The target peer-recovers
  from the source, and the source's tracker follows it. Then:
  1. the source blocks every operation permit (`IndexShard.relocated`, `:1126`);
  2. it hands its primary context to the target: checkpoints of the in-sync copies, and the routing table
     (`ReplicationTracker.startRelocationHandoff`, `:1788`, called at `RecoverySourceHandler:924`);
  3. the target becomes primary with that context (`activateWithPrimaryContext`, `:1847`);
  4. the source leaves primary mode, and a write that reaches it is sent on to the target
     (`TransportReplicationAction:579–590`).

  The term does not change. The two copies never both take writes, because the source has stopped before the
  target starts. Writes pause briefly and none is lost. The other replicas stay in the group, because the target
  inherits the source's checkpoints rather than starting from its own replica-mode view (see §4 below).

**Recommendation:**

- **Target state, a docrep drain is a relocation** of each primary off the draining node. This is "lean on vanilla"
  in the most literal sense: the server code that does the hard part exists and is exercised by every OpenSearch
  cluster.
- **Until then, demote-then-promote, sequenced:** write A's `REPLICA` at N+1, wait until A's inventory no longer
  reports a primary, then write B's `PRIMARY` at N+1. Nothing is ever primary twice. For a few poll intervals nothing
  is primary, and writes retry. This is the failover M9 proved on four nodes, with "A stopped" replaced by "A
  demoted", so it needs only the one-primary rule above.
- **Not recommended:** keeping one pass and adding a per-copy primary term to the inventory and projection, so that a
  node seeing two primaries keeps the higher-term one. It closes the gap, but it is our own substitute for the atomic
  cluster state vanilla relies on, and it still loses the other replicas from the group (§4).

### 3. What a relocation needs

Written for the supervisor to weigh against §29.3. None of it is built.

- **The target is on a node with no copy of the shard.** A node holds one copy of a shard (`IndexShardRoutingTable:1150` asserts it), and
  `SameShardAllocationDecider` enforces it upstream. §29.3 already authorises one extra copy on a node not draining,
  for the case where no in-sync replica is outside the drain. That seam fits here, with the extra copy placed as the
  relocation target instead of as a replica that is then promoted.
- **Both nodes fabricate the same relocating entry.** It carries the source's allocation id and the target's.
  Recommended: the source's node mints the target's id when it applies "relocating to D", and publishes it in its
  inventory. The target's node builds its copy once the projection shows it. This is the existing rule, routing each
  copy under ids its own node publishes, extended to a pair. It is a contract change, so it needs an entry in
  `NODE_CONTRACT.md`.
- **Completion.** The target's start is `LocalShardStateAction.shardStarted` on a relocation target, which that class
  already handles (lines 131, 181, 220). The supervisor then sees the target started as primary, and rewrites both
  assignments: the target as a plain `PRIMARY` at the same term, and the source withdrawn.
- **The cost is the target's recovery.** Docrep copies merge independently, so a node with no copy receives nearly
  every file. That cost replaces the demoted copy's rebuild in the proposal, rather than adding to it.
  - **A possibly cheaper variant, reasoned and untested:** relocate onto the node of an existing replica, after
    closing that replica. Peer-recovery retention leases are keyed by node (`ReplicationTracker:596`), so the target
    could recover by operations from the replica's old files. It costs a copy for the length of the move, so it is
    worth measuring before choosing.
- **`intent: return` keeps its copy count** during the move, since the target replaces the source. The returning
  node comes back without that shard, and §14.5 decides what to place there.

### 4. Other things found while checking

- **Rule 1 already holds, with no record of the demoted id.** `CoordinatorState` skips every projected copy on the
  local node (line 250), so a table's primary is never the node's own old copy, and with no other primary
  `withAPrimaryForEveryShard` leaves the initializing copy out until one appears. The gap is `PeerRecovery.primaryVisible`,
  which counts the local node's own copy as a visible primary. So the replica is built, then dropped with no reason
  reaching `applied`. This side fixes that by ignoring local copies there.
- **A promotion rebuilds every replica, not only the demoted one.** A replica's metadata lists only its own id as in
  sync (`DataNodeState:402`), so a promoted primary's tracker knows only itself. It leaves the other replicas out,
  and `StaleReplicas` rebuilds them. That is true of M9's failover today, and of the sequenced fallback.
- **Whether those rebuilds are cheap depends on timing.** Right after promotion, the new primary's table holds only
  itself, all started. `ReplicationTracker.getRetentionLeases(true)` (`:300–307`) then expires the leases of every
  other node at the next retention-lease sync (`index.soft_deletes.retention_lease.sync_interval`, 30 s by default),
  unless the rebuilt copies reach the table first. So the `GET _recovery` check in this proposal can come out either
  way from run to run, and should be read with that in mind. A relocation avoids both problems, since the target
  inherits the source's group.

### 5. The fence and `remoteShardFailed`

- **Confirmed from the source:** an operation at an older term is refused as "too old" before
  `assertReplicationTarget` runs (`IndexShard:5540–5552`), so a promoted primary rejects the old primary's
  operations cleanly.
- **Agreed: do not fix the hang by dropping the rejecting copy locally.** This side's `CLAUDE.md` recommended exactly
  that, and is corrected in the same commit as this reply.
- **But the old primary cannot see the newer term.** Its own state still says N. The N+1 appears only in the message
  of an `IllegalStateException` from the replica, and under `-ea` upstream asserts that the failure handed back is a
  `NoLongerPrimaryShardException` (`ReplicationOperation:376`). **Suggested:** hold the listener until the next pass,
  which has read the assignment. If the copy is still primary at the same term, drop the failed replica and complete
  the write. If it has been demoted or withdrawn, fail with `NoLongerPrimaryShardException`. The wait is at most one
  poll, and the answer comes from the assignment, which is the authority. With relocation, drains no longer depend on
  this, so it matters for failover only.

### 6. Tests worth having, revised

- the new primary's table on its promotion pass, with the old copy still projected `PRIMARY`, has exactly one
  primary (fails today);
- a node whose primary is still running, and which sees another copy projected `PRIMARY`, keeps replicating to it
  rather than dropping it from its in-sync set;
- `PeerRecovery` declines a replica, with a reason, while the only projected primary is this node's own copy;
- for relocation, once built: the relocating pair is fabricated identically on both nodes, and twice running;
  `StateEquivalence.sameContent` holds across the handoff; a write in flight during the handoff is acknowledged once,
  by the target;
- on a running cluster: a drain of one docrep primary loses no acknowledged write, and the shard's other replicas
  are not rebuilt.
