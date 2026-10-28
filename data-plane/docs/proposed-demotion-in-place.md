# Proposed: demoting a docrep primary in place

**From the supervisor, 2026-10-08.** A proposal for this component, in the form `../control-plane/docs/proposed-*.md`
uses. It is the one new obligation `../supervisor/DESIGN.md` §29 puts on the data plane for a drain, and it is
smaller than it looks, because most of it already happens by accident. Everything said about OpenSearch below was
read from 3.10.0-SNAPSHOT's source, with the place named.

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
