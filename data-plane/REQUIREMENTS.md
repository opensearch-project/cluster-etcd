# Data plane requirements

Numbered `D*` so they can be cited without colliding with the supervisor's `R*`. Each is meant to be
falsifiable: if you cannot imagine the test, the requirement is badly written and worth pushing back on.

Where this file and [`../opensearch-supervisor/NODE_CONTRACT.md`](../opensearch-supervisor/NODE_CONTRACT.md)
disagree about a document's shape, **that file wins** — it is the contract the supervisor is already built
against, and it has been implemented twice already.

---

## D1. What this is

- **D1.1** An OpenSearch plugin. Not a sidecar, not a wrapper process: it runs inside the node it manages,
  because the thing it has to do — hand the node a new `ClusterState` — is only reachable from inside.
- **D1.2 There is no cluster manager in the deployment, and no election.** The plugin declares
  `ClusterPlugin.isClusterless()`. Nothing anywhere should be waiting to discover a manager, and a node must
  become fully functional without ever having met a peer.
- **D1.3** A node's role — data, coordinator, or both — comes from its assignment and registration, not from
  local configuration alone. A coordinator holds no shards and needs the routing table for its tenants; a data
  node holds shards and needs to know almost nothing about the rest of the cluster.
- **D1.4** The plugin holds **no persistent state of its own** beyond what OpenSearch already keeps on disk.
  Everything it needs to decide anything is either in the store or in the node's own data directory.

## D2. Convergence

- **D2.1 Level-triggered, always.** Every decision is a function of state as most recently observed, never of
  the event that prompted the look. A watch notification may cause the plugin to look sooner; it must never be
  the reason it acts, and losing every notification must cost latency and nothing else.
  *Test: drop all watch events and assert convergence still happens on the poll interval.*
- **D2.2** Applying the same observed state twice must be indistinguishable from applying it once. In
  particular a re-applied assignment must not restart a shard that is already serving it.
- **D2.3** An assignment is a **complete statement of desired state**. A shard named in it that the node does
  not hold must be created; a shard the node holds that is *absent* from it must be dropped. Absence is an
  instruction, which is what makes removal expressible without a new verb.
- **D2.4** A shard copy that cannot be created must leave the node reporting that fact and continuing to serve
  everything else. One unrecoverable shard must never stop a node applying the rest of its assignment.
- **D2.5** The plugin must converge from an empty local disk (a new node) and from a disk holding shards that
  are no longer assigned to it (a returning node), without special-casing either.

## D3. What the plugin writes

Three documents, split by how often they change, for the reasons in `NODE_CONTRACT.md`. **The split is not
negotiable**: 200 nodes writing everything at heartbeat rate is the store's dominant write load.

- **D3.1 `nodes/registration/<node-id>`** — written on start and when an attribute changes. Carries a fresh
  `ephemeral_id` per process, which is how the supervisor tells a restart from a continuous run.
- **D3.2 `nodes/heartbeat/<node-id>`** — written every interval. Small, fixed shape, 4 KiB budget. Carries
  capacity, the applied-assignment outcome, and the **digest** of the inventory rather than the inventory.
- **D3.3 `nodes/shards/<node-id>`** — the inventory: what this node *has*, per shard copy, with role, state and
  size. **Written only when it changes.**
- **D3.4 `generated_at` on the inventory means "when this last changed", not "when it was last computed."** A
  node that stamps the current time on every recomputation produces new bytes and a new digest every interval,
  which puts the document that scales with shard count back onto the hot path and defeats D3.2 entirely.
  *Found by implementing it once already; do not rediscover it.*
- **D3.5** `size_bytes` is **omitted rather than zero** while a copy is initializing. Zero is a real size and
  would tell a disk-aware decider the copy costs nothing.
- **D3.6 A node that would exceed the inventory's size budget must write nothing and report `outcome: failed`.**
  A truncated inventory is worse than a missing one: the supervisor reads absent copies as *gone* and starts
  rebuilding shards that exist.
- **D3.7 The heartbeat interval a node declares must be the interval it actually writes at.** The supervisor
  derives its liveness thresholds per node from that number, so a node declaring 1s and writing every 30s will
  be declared lost and have its shards rebuilt elsewhere.
- **D3.8** The inventory must report copies that are **not yet started**, with their state. A projection
  filtered to started copies only would deadlock docrep recovery — see D5.3.

## D4. What the plugin reads

- **D4.1 `nodes/assignment/<node-id>`** — its own, and no other node's. A node reading another node's
  assignment would be a second reader of a single-writer document and would couple the two.
- **D4.2 `tenant/<t>/indices/<name>`** — index settings and mappings for the indices it serves. Settings are
  **flat, and every value is a string**: `"3"`, not `3`. Reading `"3"` as an int the wrong way silently yields
  zero.
- **D4.3 `tenant/<t>/status/indices/<index-uuid>`** and **`tenant/<t>/status/nodes/<node-id>`** — read only if
  the node needs peers: a coordinator routing traffic, or a document-replication copy resolving its primary.
  Node addresses come from the second; do not resolve a peer by reading its heartbeat.
- **D4.4 Index identity is the uuid, never the name.** Names are reused. A node keyed on the name will
  eventually confuse an index with its deleted predecessor and serve the wrong data.
- **D4.5 A torn read is normal and must never be treated as corruption.** `scanPrefix` is not
  snapshot-consistent on every backend — measured: on ZooKeeper almost every scan straddles concurrent writes.
  An inconsistent read must cost a pass, not an error.
- **D4.6** An unreadable or absent document must be distinguishable from an empty one, and must never be read
  as "nothing is wanted." Absent means *unknown*; it does not mean *none*.

## D5. Talking to OpenSearch

- **D5.1** Desired state is applied through `ClusterApplierService`, in the way an ordinary published cluster
  state would be, so that every existing recovery, allocation and search path runs unmodified. The plugin
  fabricates a `ClusterState`; it does not reimplement what the server does with one.
- **D5.2** Cluster-manager-only actions must be answered locally or refused with a clear reason, never left to
  hang waiting for a manager that does not exist. `cluster.health` and `cluster.state` are answerable from the
  supervisor's projections; anything that would *mutate* metadata belongs to the control plane and must be
  rejected as such.
- **D5.3 A copy whose peer is not ready yet must wait, not fail.** When a primary and a replica are placed in
  the same pass, the replica will read the routing projection before the primary is serving. It must stay
  `INITIALIZING` and try again. This is why D3.8 exists.
- **D5.4** Local shard data must survive an assignment that stops naming it, at least long enough to be
  reported. What the node does with the bytes afterwards is its business, but the supervisor's statement is only
  "stop serving this", and treating it as "destroy this immediately" removes the cheapest recovery path there
  is.
- **D5.5** The plugin must not require a restart to pick up a changed assignment, a changed index setting, or a
  changed pool membership.

## D6. Persistence

- **D6.1 The store is reached through the `KeyValueStoreProvider` SPI**, exactly as the control plane and
  supervisor reach it. One store URI setting names a scheme; the backend is a jar on the classpath, discovered
  by `ServiceLoader`.
- **D6.2 Nothing in this repository may name a backend** outside test code and the SPI wiring. No compile
  dependency on jetcd or Curator. `--help`-equivalent diagnostics should list the schemes actually found, so a
  missing jar is distinguishable from a typo.
- **D6.3** Revisions are **opaque** and comparable only for equality. Never order them, never do arithmetic on
  them, never assume they increase by one.
- **D6.4** Every write the plugin makes to a document it owns is unconditional; every write to a document it
  shares is a compare-and-set. Today it shares none, which is a property worth preserving deliberately.
- **D6.5** A store that is unreachable must leave the node **serving what it already has**. Losing the store is
  losing the ability to *change*, not the ability to work. A node that stopped answering queries because it
  could not reach a coordination service would make the store a single point of failure for the data path.

## D7. Observability

- **D7.1** Every published document carries the node's own view of *why* it is in the state it is in — the
  `applied` outcome, the failure detail on a failed copy — because the supervisor's diff is only as good as
  what it can read.
- **D7.2** Failure detail must be bounded in length. An unbounded reason field on a per-copy document is how a
  size budget gets exceeded during exactly the incident that produced the failures.
- **D7.3** A node must be able to say, locally, what it last read, what it last applied, and what it last
  wrote. The supervisor's projections answer "what does the fleet look like"; this answers "why is *this* node
  behaving like that", and the second question is asked at 3am.

## D8. Safety

- **D8.1 The plugin must not promote itself.** It holds the role it was assigned. Deciding that a shard needs a
  primary is the supervisor's job, and a node that promoted on its own initiative could produce two primaries
  from one partition.
- **D8.2 Primary fencing is a known, open gap — do not paper over it.** There is currently no per-primary
  generation, so two nodes briefly believing they are primary for a remote-store-backed shard could write to the
  same remote path and corrupt it. The supervisor avoids the situation by never moving or failing over a
  primary. **If implementing this exposes a mechanism that would let a superseded primary be fenced, that is a
  finding worth reporting** — it is the single biggest gap in the whole design, and it is partly work in
  OpenSearch itself. See `../opensearch-supervisor/DESIGN.md` §13, open question 1.
- **D8.3** A node must never act on an assignment written for a different `ephemeral_id` in a way that assumes
  continuity with its own predecessor's state.
- **D8.4** Two processes must never share a node id. If a node id is derivable rather than configured, it must
  be derived from something that cannot collide.

## D9. Testing

- **D9.1 The plugin and `fake-node` must satisfy the same contract.** `fake-node` is the reference reader of
  `NODE_CONTRACT.md`; the plugin is the real one. **Recommended: extract the contract assertions into a shared
  suite both can run**, in the manner of `KeyValueStoreContractTests` in the store repository. Two
  implementations of one document format that are only tested separately are two formats.
- **D9.2** The supervisor's `supervisor-integ` module already starts a real etcd and a real ZooKeeper under
  Testcontainers and drives real supervisors against them. **A plugin node standing in for `fake-node` there is
  the end-to-end test that matters**, and is worth aiming at early rather than at the end.
- **D9.3** Tests must not depend on wall-clock sleeps where a controllable clock would do, and must not depend
  on a controllable clock where the point is a real one. The supervisor learned this the expensive way: its unit
  suite advances a fake clock, and a separate integration suite exists precisely because that leaves liveness
  timing unexercised.
- **D9.4** At least one test must run with **every watch notification dropped**, to keep D2.1 honest.

---

## Open questions — decide these early, they are cheap now

0. **Answered by building milestones 1 and 2** — recorded here rather than deleted, so the reasoning
   survives the decision.

   - **Setting names (question 1).** `clusterless.store`, `clusterless.node.pool`,
     `clusterless.node.roles`, `clusterless.heartbeat.interval`, `clusterless.poll.interval` — the proposal
     below, adopted as written, plus `clusterless.node.roles`. That last one exists because the two role
     vocabularies do not line up: OpenSearch has no `coordinator` role, and a coordinating-only node is one
     with *no* roles at all. The derivation is `data` if the node has OpenSearch's data role, `coordinator`
     if it has no OpenSearch roles, and the setting overrides both. Note what the derivation deliberately
     does not do: every OpenSearch node *can* coordinate a search, so deriving from capability would make
     the whole fleet a coordinator for every tenant.

     **`clusterless.node.tenants` is deliberately not implemented.** Which tenants a node serves is in its
     assignment, which is the supervisor's to decide; a node that also declared its tenants locally would
     give the question two answers, and the interesting case is precisely the one where they differ.
   - **Poll interval versus heartbeat (question 2).** Separate settings, sharing a default. Different
     costs — a heartbeat is a write by every node in the fleet, a poll is a read of one small key — so
     tying them would force whichever number is wrong for one of them.
   - **Does a coordinator heartbeat (question 3).** Yes, with an empty inventory. Nothing in the publisher
     is conditional on holding shards, because liveness and pool membership both key off the heartbeat and
     an empty inventory is a fact worth publishing.
   - **Mapping canonicalisation (question 4).** No, current OpenSearch offers no cleaner path: neither
     `createComponents` overload receives `IndicesService`, and it is not in the plugin component registry
     either. Checked against 3.9. The `GuiceHolder` hack is copied, with the reasoning recorded on it —
     shard sizes need it now and mapping canonicalisation will need it at milestone 4.
   - **Replication story for the first version (question 6).** **Segment replication from the remote
     store**: primaries and search replicas, no peer resolution, document replication deferred. This is
     the sharpest possible exposure to D8.2, which is a reason to be explicit about it rather than a reason
     to choose otherwise.

1. **Setting names.** Proposal: `clusterless.store` for the store URI, `clusterless.node.pool`,
   `clusterless.node.tenants`. `cluster-etcd` used `cluster.etcd.endpoint`, which names a backend and so cannot
   survive D6.2. Worth agreeing with the supervisor and control plane so all three read alike.
2. **Poll interval versus watch.** The heartbeat interval and the assignment poll interval are different
   numbers with different costs. Are they configured separately?
3. **Does a coordinator heartbeat?** It holds no shards, so its inventory is empty — but the supervisor's
   liveness and pool membership both key off the heartbeat, so presumably yes, with an empty inventory.
4. **Mapping canonicalisation.** `cluster-etcd` routes mappings through `IndicesService` to canonicalise them,
   reached via a Guice holder. Is there a cleaner path in current OpenSearch?
5. **How much of `cluster.state` should be answerable?** The supervisor projects enough for health and routing.
   Anything beyond that is a new requirement on the supervisor, not something to synthesise here.
6. **Ingestion.** Pull-based ingestion is the preferred replication story and it changes what a "primary"
   means — multiple copies can pull from one source concurrently. Does the first version support it, or docrep
   push only, or segrep-from-remote-store only? This choice constrains D8.2 more than anything else here.
