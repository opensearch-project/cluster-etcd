# Findings from the data plane

Eight items wanted in this repository, all found by running the real data plane
(`../opensearch-data-plane`) against a real supervisor, a real control plane and one store. None loses data.

The first two are cases where **the supervisor made a decision and did not say so** — the failure mode this
design spends most of its effort trying to avoid — so they are worth closing properly rather than patching. The
third was a missing field that made a projection unable to serve either of the two readers it was written for; it
is **done** as of `50ecdf5`, and reading it is still worth it for why an address was not sufficient. The fourth
blocks the milestone the third unblocked: nothing ever assigns the coordinator role, so the routing this enabled
cannot be exercised. The eighth is not a defect at all — it is this side's answer to `DESIGN.md` §21.2, agreeing
with the acknowledgement design and reporting that the sequencing underneath it cannot work as written.

Each item says what happened, why the current behaviour is wrong, where the code is, what a fix has to be careful
about, and the tests worth having. All of them are self-contained.

---

## 1. `index.number_of_search_replicas` is read by nobody

> **Closed.** Option (1): the setting is honoured, copies are counted per role, and a request the supervisor
> declines now leaves a finding behind. Details at the end of this item.

### What happened

A tenant asked for an index with segment replication and a search replica, spelling it the way OpenSearch spells
it:

```json
{"index.number_of_shards": "1", "index.number_of_replicas": "0",
 "index.number_of_search_replicas": "1", "index.replication.type": "SEGMENT"}
```

The supervisor planned **one copy**, placed the primary, and projected the index as **green with zero unassigned
shards**:

```json
{"config_revision": 4, "replication": "SEGMENT", "number_of_replicas": 0,
 "shards": {"0": [{"node_id": "UntRkFd4...", "role": "PRIMARY", "state": "STARTED"}]},
 "unassigned_shards": 0, "status": "green"}
```

The `config_revision` shows it had read the updated document. It simply has no code that looks at that setting, so
the request evaporated: no copy, no finding, no complaint, and a status saying everything asked for had been
delivered. The data node was healthy and idle throughout, and there was nothing anywhere to read that would
explain why.

### Why the current behaviour is wrong

Not the mapping — deriving the *role* of a replica from `index.replication.type` is right, and the data plane
agrees with it exactly (`SEGMENT` → `SEARCH_REPLICA`, `DOCUMENT` → `REPLICA`). The problem is that a setting a
user can legitimately write is silently inert, and the projection then claims success. An operator's only route to
the truth is reading this repository's source.

It also cannot express a real configuration. In OpenSearch the two counts are independent: a segment-replication
index may have both ordinary replicas (peer-recovering, primary-eligible) and search replicas (read-only, never
promoted). `copiesPerShard()` is one number and cannot say that.

### Where the code is

- `supervisor-model/.../index/IndexConfig.java` — `SETTING_REPLICAS = "index.number_of_replicas"` is the only
  such constant; `copiesPerShard()` returns `1 + numberOfReplicas`; `copiesAuthorisedFor(shard)` builds on it.
  `number_of_search_replicas` appears nowhere in the module.
- `supervisor-model/.../plan/Planner.java` — `replicaRoleOf(index)` picks the role; the planner's skip is
  `context.nodesWithThisShard().size() >= index.copiesAuthorisedFor(shard)`.
- `supervisor-model/.../diff/Differ.java` (~161–171) — `nonPrimaries < index.numberOfReplicas()` raises
  `UNASSIGNED_REPLICA`, and the same number decides `SURPLUS_COPY`.
- `supervisor-model/.../status/IndexStatus.java` (~82) — `desired = numberOfShards * (1 + numberOfReplicas)`,
  which is what made the index green.

### What to do

Either answer is defensible and they are not equally cheap:

1. **Honour it** — parse `index.number_of_search_replicas`, and let a shard's desired copies be one primary plus
   `numberOfReplicas` plus `numberOfSearchReplicas`. This matches OpenSearch and expresses the configuration that
   currently cannot be expressed. It touches more than the count: the planner needs to know *which* kind of
   replica each missing copy is (a docrep replica and a search replica of one index are different findings with
   different placement rules — `ReplicaAfterPrimaryDecider` waits for a primary for one and not the other), so
   `UNASSIGNED_REPLICA` probably has to carry the role, or become two kinds.
2. **Refuse it** — have the control plane reject the setting, keeping `number_of_replicas` as the single knob
   whose meaning depends on the replication type. Cheaper, and it makes the request fail loudly at the boundary
   instead of vanishing. It does leave "both kinds of replica" inexpressible.

**Recommended: (1), and treat (2) as the fallback if the finding-per-role work is more than it is worth.** The
independent-counts model is OpenSearch's, tenants will write it, and the data plane already handles both roles
per shard.

**Whichever is chosen, the general defect is the silence, and it deserves its own fix:** a copy that the desired
state asks for and no plan ever proposes should raise a finding, not be omitted from the arithmetic. Today the
count *defines* what is wanted, so anything outside it is invisible by construction. A `DESIRED_COPY_IGNORED`
finding (or similar) covering "a setting asked for copies this supervisor does not plan" would have turned a
code-reading exercise into a line in the diff. That is the same discipline as the data plane's
`assignment.deferred`: every copy it declines to build leaves a reason behind.

### What was done

Option (1), with the mapping put in one named place rather than spread out.

- **`IndexConfig.desiredCopiesOf(ShardRole)`** is now the only thing that maps the two settings onto roles, and
  carries the reasoning. `copiesPerShard()` is derived from it.
- **`number_of_replicas` keeps its existing meaning** — `SEGMENT` → `SEARCH_REPLICA`, `DOCUMENT` → `REPLICA` —
  because the plugin dispatches on the assigned role and *defers* a `REPLICA` today. Changing it would have
  turned working search replicas into deferred ones, which is a change both sides have to make together. So on a
  segment-replication index the two counts are **added** and both ask for search replicas; "one ordinary replica
  and one search replica" is still inexpressible, now recorded as `DESIGN.md` §13 open question 10 rather than
  being invisible.
- **`UNASSIGNED_SEARCH_REPLICA`** is its own finding kind, because the planner dispatches on kind to choose a
  role and because the counts read better apart. `Planner.replicaRoleOf` is gone: the role comes from the kind,
  so the mapping is applied once, by the differ.
- **Counting is per role, not in total**, in the differ, the planner's ceiling
  (`AllocationContext.copiesOfThisShardInRole`) and `SurplusReaper` — which now drops a copy of a role that is
  itself over rather than any non-primary. Two counts that sum correctly can both be individually wrong.
- **`IndexStatus`** carries `number_of_search_replicas` and counts it as desired, which is what makes the index
  yellow rather than green while the copy is missing.
- **`DESIRED_COPY_IGNORED`** is the general fix asked for. Its first instance: search replicas on a
  document-replication index, which cannot exist because a search-only copy recovers from a remote store. Those
  copies are reported and deliberately **not** counted as unassigned — unassigned means "intended and not yet
  placed", and a permanently unplaceable request reported that way would leave the index yellow for ever with
  nothing an operator could do.

Four existing tests changed kind rather than behaviour, all of them fixtures that had said `REPLICA` on a
segment-replication index while the count was checked in total and the role ignored. That was worth seeing.

**Not done:** the placement rule from finding 7 — do not plan a `SEARCH_REPLICA` onto a node with no segment
repository, and treat disagreeing repositories in one pool as a finding. Still open, and now more useful than it
was, since search replicas are actually planned.

### Tests worth having

- an index with `number_of_search_replicas: 1` and `number_of_replicas: 0` plans two copies, one `PRIMARY` and
  one `SEARCH_REPLICA`;
- the same index is **not** green while the search replica is unplaced — `IndexStatus` counts it as desired;
- `number_of_replicas: 1` plus `number_of_search_replicas: 1` on a `SEGMENT` index plans three copies with both
  replica roles, and the docrep-only ordering rule applies to just the one that needs it;
- whatever is chosen, a setting the supervisor does not act on produces a finding rather than nothing.

---

## 2. `unplaceable_because` shows one example refusal, and it can be the least useful one

### What happened

A search replica stayed unplaced for hundreds of passes. The finding was correct and its explanation was not
actionable:

```json
{"kind": "UNASSIGNED_REPLICA",
 "because": "index [my-index] shard 0 has 0 of 1 replicas; 2 node(s) in this pool could take it",
 "unplaceable_because": "no node was allowed by every decider; 2 candidate(s) refused, for example node
   [UntRkFd4...]: [same_shard] node [UntRkFd4...] already holds, is assigned, or is about to be given a copy of
   this shard; a second copy on one node adds no redundancy at all"}
```

Two candidates were refused for **different** reasons. The one shown is the node legitimately holding the primary,
refused by the rule that is obviously right and tells nobody anything. The refusal that actually blocked progress
— the other node, refused for sharing an address with the primary's node — was not shown. Diagnosing it took
reading `SameShardDecider`.

### Why the current behaviour is wrong

The refusal text is otherwise excellent: this codebase's rules explain themselves, and `Planner` even has a
comment noting that an earlier version computed the reason and threw it away. Having built that, showing an
arbitrary one of *n* distinct reasons wastes it — and the arbitrary choice is biased towards the boring one,
because the node already holding the copy tends to sort first and is always refused.

The cost is exactly the cost this repository's observability is meant to prevent: a shard that stays unassigned
while the diff insists nodes could take it.

### Where the code is

- `supervisor-model/.../allocate/Allocator.java` — `refusalBecause(...)` composes the sentence and `first(...)`
  picks the example with `verdicts.entrySet().stream().findFirst()`.
- `Decision.Combined.summary()` renders the deciding decisions as `[decider] reason; [decider] reason`, and
  `Decision.deciding()` already narrows to the blockers — so a distinct-reason key is available without new
  plumbing.
- `supervisor-core/.../Projector.java` (~84–200) — carries `unplaceableBecause` into the projected finding.

### What to do

Show **one example per distinct refusal reason**, capped, rather than one example overall. Suggested shape:

> `no node was allowed by every decider; 5 candidate(s) refused for 2 distinct reason(s): [same_shard] node [A]
> already holds ... (1 node); [same_shard] node [B] shares address 127.0.0.1 with node [A] ... (4 nodes)`

Points to be careful about:

- **Group on the decider plus the shape of the reason, not the whole string**, or every node produces a "distinct"
  reason because its own id is in the text. Grouping on decider name alone is probably too coarse — the example
  above shows one decider giving two genuinely different answers — so consider the decider name plus the
  `Decision` list's deciders, with one node's full text as the illustration.
- **Keep it bounded.** This string is projected into the store and read by the control plane; a pool with hundreds
  of nodes must not produce hundreds of clauses. Cap the distinct reasons (`Differ`'s existing truncation
  discipline is the precedent) and say how many were omitted.
- **Preserve the throttled-only case as it is.** "Every candidate is throttled, so this is paced rather than
  blocked" is already the right summary and should not be diluted by grouping.
- **`REQUIREMENTS.md` §7 is the justification** if one is wanted: observability here is a requirement, not a
  nicety.

### Tests worth having

- two candidates refused by different reasons: both appear;
- twenty candidates refused by the same reason: one clause, with the count, not twenty;
- the reason that blocked progress appears even when a less informative one sorts first — the regression this came
  from;
- the throttled-only sentence is unchanged.

---

## 3. `tenant/<t>/status/nodes/<id>` cannot produce a node object that can connect

**Do this one first if any of the three is ordered: it blocks the data plane's milestone 8 (coordinators), and it
also defeats this projection's own stated purpose.**

### What is missing

`NodeStatus` carries `nodeId`, `name`, `roles`, `address`, `port`, `version`, `attributes`, `liveness` and
`observedAt`. It does not carry the node's **ephemeral id**, and without it a reader cannot build a
`DiscoveryNode` that OpenSearch will connect to.

Two lines of server code decide this:

- `DiscoveryNode.equals` (server, ~line 594) compares **only** `ephemeralId`. Not the node id, not the address —
  just that one field.
- `TransportService.connectionValidator` (~line 619) handshakes and then throws
  `ConnectTransportException(node, "handshake failed. unexpected remote node " + remote)` unless
  `node.equals(remote)`. `NodeConnectionsService` runs that validator for every node in an applied cluster state.

So a coordinator that reads this projection can learn exactly where a data node is and still be unable to talk to
it: the `DiscoveryNode` it constructs is unequal to the one the data node reports about itself, and the connection
is refused during the handshake. An address is necessary and not sufficient.

This is not coordinator-specific. `NodeStatus`'s own javadoc names its second reader as "a document-replication
data node turning a peer's node id from `IndexStatus` into an address it can actually connect to", and
`NODE_CONTRACT.md`'s *Routing is not here, and neither is peer wiring* says node addresses come from this
document. Both statements are true about the address and wrong about the outcome — which means milestone 9 hits
the same wall.

### What to do

**Add `ephemeral_id` to `NodeStatus`, from the registration the supervisor already reads.** `nodes/registration/
<id>` and `nodes/heartbeat/<id>` both carry it, so the field costs one more string in a document that is already
being written and no new read.

Points to be careful about:

- **It changes on every node restart**, by design — it identifies an *incarnation*, which is exactly why the
  transport uses it. So the projection must be rewritten when a node's ephemeral id changes, on the same trigger
  as any other registration change. A stale value means a coordinator cannot connect until the projection catches
  up; that is the ordinary level-triggered lag and the data plane will report it as a copy it could not route to,
  rather than retrying silently.
- **Do not let this justify inlining more of the heartbeat.** The reason to add this one field is that the
  transport makes it structurally necessary; load and capacity figures remain firmly in
  `nodes/heartbeat/<id>`, and the "slow-changing facts only" rule in the javadoc still holds — an ephemeral id is
  as slow-changing as a node's identity.
- **The alternative was considered and rejected**, and it is worth writing down so it is not revisited: the data
  plane could read `nodes/registration/<id>` for each peer instead. `NODE_CONTRACT.md` argues against exactly that
  — it makes every node a reader of every other node's document, couples the data plane to the heartbeat schema,
  and gives a single-writer document a class of reader its author never considers. That argument is still right;
  the fix belongs in the projection.
- `SUPERVISOR_KEYS.md` documents this key and should gain the field in its example.

### Tests worth having

- the projection carries the ephemeral id the node registered with;
- restarting a node — new ephemeral id, same node id — updates the projection rather than leaving the old value;
- a `LOST` node's projection is still whatever was last observed, since `isReachable()` already distinguishes that
  and a reader deciding not to connect is different from a reader unable to.

### Why the data plane cannot work around it

A fabricated ephemeral id cannot work: any value other than the node's own fails the equality check, so there is
no "best effort" available. Until the field exists, a coordinator can enumerate its tenants' shards, build index
metadata and a routing table, and then has to decline to route to each copy with a reason naming this document —
which is honest and useless. That is why this is first.

---

## 4. Nothing ever assigns the coordinator role

> **Closed.** Both halves, taking recommendation (1): `roles` is a set per tenant entry, and coordinator entries
> are computed in the applier as membership. Notes at the end of this item.

**This blocks the data plane's milestone 8 from being tested at all**, in the same way item 3 blocked building it.
The routing side is now written and unit-tested; it cannot be exercised against a real supervisor because no
supervisor ever tells a node to coordinate.

### What is missing

`Assignment.TenantAssignment.coordinator()` exists and its only caller in the repository is
`NodeJsonTests`. `Applier` builds tenant entries from two instructions — `Instruction.PlaceCopy` and
`Instruction.RemoveCopy` — and gives a new entry `NodeRole.DATA`:

```java
tenants.put(place.tenant(), new Assignment.TenantAssignment(
    tenant == null ? NodeRole.DATA : tenant.role(), indices
));
```

So a node that registers with `roles: ["coordinator"]` is observed, counted, found placeable for nothing, and
written no assignment — because assignments are produced by placement and there is nothing to place on it. Its
`coordinatorTenants` is empty for ever, and the data plane's coordinator path never activates.

### Why it does not fit the existing flow, which is the interesting part

Every assignment today is the result of a **finding**: the diff says a copy is missing, the planner places it, the
applier writes it. A coordinator entry is not like that. There is no shard to place, so there is no finding to
raise, and inventing one per (node, tenant) would flood the diff with items that are never *wrong* — a coordinator
without a tenant is not a deficiency, it is a node waiting to be told what it is for.

It is a **membership** statement rather than a placement one: *for each node in this pool with the coordinator
role, for each tenant bound to this pool, the assignment should carry `role: coordinator`.* Both inputs are already
in the observation. Suggested shape: compute it in the applier where the assignment document is assembled, as a
property of the assignment rather than as an instruction, so it does not consume the work ceiling — nothing moves,
and a pass that spent its budget placing shards should still tell a new coordinator what it serves.

Level-triggered, with the removal half mattering more than it looks: a tenant rebound to another pool must have its
coordinator entry **removed**, or a coordinator keeps routing to shards a different supervisor now owns. That is
the same hazard `DESIGN.md` §4 discusses for rebinding, arriving through a document nobody currently writes.

### The second half: a node cannot be told it is both, for one tenant

`TenantAssignment.role` is a single `NodeRole`, and `NODE_CONTRACT.md` reads *"a tenant entry with `role:
coordinator` and no `indices` means serve queries for this tenant, hold no data"*. So the format can express
"holds data for tenant T" or "coordinates for tenant T" and not both.

Both is a real configuration, and it is the one the data plane implemented first because it is the only one
testable on a small deployment: `CoordinatorState` decorates the data path precisely so that a node holding copies
of an index and coordinating for its tenant gets one routing table containing its own copies and the others'. A
node that can only be told one thing per tenant cannot ask for that.

Two ways out, and this is the contract's call rather than the data plane's:

1. **Make the role a set** — `"roles": ["data", "coordinator"]` per tenant entry. Says exactly what is meant, and
   costs a format change plus a tolerant reader on both sides.
2. **Let `DATA` imply the capability**, and rule that a node whose *registration* declares the coordinator role
   coordinates for every tenant it has any assignment for, plus any tenant carrying an explicit `coordinator`
   entry. No format change; the registration is where a node's capabilities already live and the contract already
   says *"`roles` decides what the node can be given"*. The cost is that a node's behaviour then depends partly on
   its own configuration rather than wholly on the instruction it was given, which cuts against the grain of
   everything else here.

**Recommended: (1).** The grain is worth keeping — a node should be able to learn everything it does from the
document written for it — and a set is what the fleet will want anyway once coordinators run on dedicated capacity
and a tenant's coordinators are not its data nodes.

### What was done

Recommendation (1) for the format, and the applier for the mechanism, exactly as suggested.

- **`TenantAssignment.roles` is a `Set<NodeRole>`**, written as `"roles": ["data", "coordinator"]`. The singular
  `role` string is still *read*, because assignments outlive the process that wrote them and an upgraded supervisor
  should not report every node's assignment unreadable for a pass. Readers wanting the same tolerance should treat
  a missing `roles` with a present `role` as a one-element set. `NODE_CONTRACT.md` now documents both.
- **Coordinator entries are computed in `Applier.applyCoordinatorMembership`**, from the node's registration roles
  and the tenants bound to the pool — membership, not an instruction, so it does not consume the work ceiling.
- **The applier no longer returns early on an empty plan.** That was the thing making this invisible: a pool with
  no shard work never told its coordinators anything, and a coordinator's job is precisely the case where nothing
  is being placed.
- **Which made a no-op guard load-bearing**, in two cases. An unchanged tenants map is not rewritten; and a node
  that was never told anything and has nothing to be told is *left alone* rather than handed an empty document,
  because an absent key already means that. Missing the second wrote an empty assignment to every idle node, which
  is how I found it.
- **Removal falls out of the level-triggered shape**: the desired set is computed from the currently bound tenants,
  so a rebound tenant is simply not in it.

One thing to check on your side: the data plane's reader needs to accept `roles`, and a node with both roles for
one tenant should get a routing table containing its own copies. A dual-role node's document now looks like this:

```json
{ "tenants": { "acme": { "roles": ["data", "coordinator"],
    "indices": { "uzKx…": { "name": "my-index", "config_revision": 6,
                            "shards": { "0": "PRIMARY", "1": "SEARCH_REPLICA" } } } } } }
```

### Tests worth having

- a pool with one coordinator-roled node and two bound tenants writes an assignment naming both, with no indices;
- a tenant rebound away has its coordinator entry removed on the next pass;
- a coordinator entry is written even in a pass whose work ceiling was exhausted by placements;
- whichever answer item 4's second half gets, a node that both holds copies of an index and coordinates for its
  tenant can be told so, and the data plane's reader agrees about what the document means.

---

## 5. The conformance suite covers only the documents a node writes

> **Closed on this side.** `ContractNode.readAssignment` exists, five checks use it, and the checks are verified to
> fail against a reader that knows only `role`. The plugin needs to implement the method — notes at the end.

### What happened

The `roles` set landed, and the data plane's coordinator immediately stopped understanding its assignment:

```
"deferred": ["default: role is not one this node understands, so nothing was done for this tenant"]
```

Its reader knew `role` and the document now said `roles`. That is an ordinary consequence of a format change and
the fix was three lines. The finding is that **nothing caught it** — not the supervisor's 500-odd tests, not the
data plane's 250-odd, and not the shared suite whose entire purpose is to stop the two implementations drifting
apart on a format. It was caught by a node failing to start doing its job, which is the most expensive place to
find it and the one the suite exists to replace.

### Why it escaped

`ContractNode` — the abstraction both implementations satisfy — is about documents a node **writes**:
`start()` announces itself, `pass()` writes whatever changed, `holdServing(...)` brings it to a state whose
inventory can be compared. Every check reads keys out of a `RecordingStore` and asserts on what the node put
there.

The assignment is the one document flowing the other way: the supervisor writes it and the node reads it. There is
no shared check that both implementations *read* it the same way, because there is no shared way to ask a node
what it understood. So the suite is complete about half the contract and silent about the other half, and the
silent half is the one where a supervisor change can break a node.

Worth noting `fake-node` cannot catch this either, for a structural reason: it writes its own assignment in order
to converge (`holdServing`'s javadoc says so), so it reads a document produced by the same code that produced its
expectations. A format change moves both together and the round trip stays green.

### What to do

Give `ContractNode` a way to be asked what it made of an assignment, and add checks that feed it documents built
by the supervisor's own writer. Sketch:

```java
/** What this node understood an assignment to be asking of it. */
record Understood(Map<TenantId, Set<NodeRole>> roles, Map<TenantId, Map<String, Map<Integer, ShardRole>>> copies) {}

Understood readAssignment(byte[] document);
```

Both sides can answer that: `fake-node` from its own parse, the plugin from `Assignment.read` plus its resolver.
The checks worth having are the ones a format change breaks:

- a `roles` array with both roles is understood as both;
- the legacy `role` string is still understood, because the two services are deployed and upgraded separately and
  for some window one of them is older;
- an unrecognised role leaves the entry understood by nothing rather than silently empty;
- shard roles and `config_revision` survive the round trip.

The last of those is the general principle: **the suite should be fed documents written by the writer, not by the
reader's own fixtures.** Hand-written JSON in either repository tests that repository's opinion of the format. The
data plane's projection codecs have the same weakness and were mitigated the same way as far as possible — their
fixtures are bytes copied verbatim out of a real store — but that is a snapshot rather than a check.

**Not urgent, and cheap relative to what it prevents.** This is the second time a cross-repo format detail has cost
a running deployment (the first being item 3's missing ephemeral id), and both times the format was documented
correctly and read wrongly.

### What was done

`ContractNode.readAssignment(byte[]) -> Understood` is added, close to the sketch. `Understood` carries
`Map<TenantId, Set<NodeRole>>` and a `List<ContractCopy>` rather than the nested triple map — `ContractCopy`
already exists for the write side and carries the index name and config revision, so the two directions are now
symmetric and a copy compares as one value.

Five checks, all fed by `NodeJson.write` rather than by hand-written JSON, except the two that deliberately cannot
be:

- both roles for one tenant are understood as both — **the regression**;
- a coordinator-only tenant is understood, with no copies;
- the singular `role` string is still understood (hand-built, because the supervisor no longer writes that form,
  which is the point);
- an unrecognised role reads as `UNKNOWN` rather than being dropped, so "understood by nothing" stays
  distinguishable from "no roles" (hand-built, same reason);
- shard roles, index name and `config_revision` survive the round trip.

**Verified by reintroducing the bug**, because an unverified conformance check is the problem this item describes.
With `TenantAssignment.fromJson` altered to read only `role`, two checks fail with
`expected: <[DATA, COORDINATOR]> but was: <[UNKNOWN]>`.

On the `fake-node` side the copies half goes through `FakeNode.desiredOf` — the node's own derivation, so it
reports what it would act on rather than what the adapter thinks the bytes say. The roles half is the bare parse,
**and honestly so: fake-node does not act on roles at all**, it walks `indices` and nothing else. It has no
interpretation of its own to report, which is exactly why these checks earn their keep on your side, where a role
decides whether a tenant is served.

### What the plugin needs to do

Implement `readAssignment` from `Assignment.read` plus the resolver, returning what the resolver *concluded* rather
than what the parser produced — the point is to catch a resolver that understood a document it parsed fine. The
`UNKNOWN` check is the one most worth getting right there: a role the plugin does not recognise should leave the
tenant present and unserved, not silently absent.

---

## 6. Pausing an index's ingestion is desired state with nowhere to live

> **The pause is done**, and the control plane got there first — it writes `ingestion_status.is_paused` at the top
> level of the index document, matching upstream's `IngestionStatus.isPaused`, **not** the `ingestion.paused`
> sketched below. The supervisor now reads it, projects it on `IndexStatus`, and completes `pause-ingestion` and
> `resume-ingestion` on the same condition as `update-settings`. Both previously parsed as `UNKNOWN` and became
> permanent `TASK_STUCK` findings.
>
> **The reset pointers are not done, and they need two things from the data plane.** `DESIGN.md` §21.2 has the
> design; the short version is that the position cannot be used to detect application — a shard reset to offset
> 1000 is at 1200 by the time anyone looks, and a rewind from 5000 to 1000 looks *already done* before it is
> applied — so the reset carries the task id into the assignment and is acknowledged through the existing
> `applied: {assignment_version, outcome}` handshake. What that needs from you: `applied: applied` must mean the
> reset landed too, and idempotence must key on the reset's `id` rather than the assignment version, which moves
> for unrelated reasons. Say the word and I will implement the supervisor half.

### What is missing

Pull-based ingestion is configured entirely through index settings — `index.ingestion_source.type`,
`.param.*`, `.pointer.init.reset` and the rest — so the data plane needed no new contract to support it. Whether a
stream is **paused** is not a setting. It is a first-class field on `IndexMetadata`:

```java
IngestionStatus(boolean isPaused)     // defaults to not paused
```

changed upstream by `MetadataStreamingIngestionStateService` through a cluster-manager action, which a clusterless
node refuses — correctly, because pausing an index's ingestion is a statement about desired state and belongs to the
control plane.

The consequence is that **a paused index comes back unpaused.** The data plane fabricates index metadata from
`tenant/<t>/indices/<name>` on every pass; that document has no field for the pause; so every pass restores the
default and the stream resumes underneath whoever paused it.

### Why the data plane cannot fix it alone

It could invent a place — a setting the control plane happens to write, or its own key — and that would be two
services holding different opinions about desired state, which is the one failure this architecture is built to
avoid. The document that says what an index should be is the control plane's, so the field belongs in it.

### What to do

Add the pause to the index document, and say who may write it. Sketch:

```json
{ "index": "logs", "uuid": "...", "state": "open",
  "ingestion": { "paused": true },
  "settings": { "index.ingestion_source.type": "kafka", ... } }
```

Points worth deciding rather than assuming:

- **Whose statement is it?** Almost certainly the control plane's, on an operator's request — the same path as
  closing an index, which this contract already models as `state`. If so, the supervisor needs nothing but to keep
  passing `config_revision` along, and the data plane reads one more field.
- **Is it per index or per shard?** Upstream is per index. Per shard would let an operator stop one hot partition,
  and nothing in the current model needs it.
- **Does it interact with `state: open`/`closed`?** A closed index holds no copies at all, so paused-and-closed is
  not a state worth having; paused means "hold the copies, keep serving reads, stop consuming".
- **What should a data node do with a pause it cannot see yet?** Nothing special: the level-triggered rule already
  covers it, since the pass after the document changes will fabricate the paused metadata. Worth stating that
  reversing it is equally ordinary, so nobody adds a latch.

### Tests worth having

- an index document with the pause set produces metadata whose `IngestionStatus.isPaused()` is true, and clearing it
  resumes — both within one pass of the document changing;
- the projection reports a paused index as green rather than as unhealthy, since it is serving reads and holding its
  copies by instruction;
- pausing is not a shard movement, so it produces no placement work and does not consume the pass's work ceiling.

---

## 7. A deleted index is never let go of, and a primary cannot displace a replica

Found by deleting an index and recreating it under the same name. The data node stopped picking up the new one
until it was restarted, and its own report said exactly why:

```
"applied": { "assignment_version": 1, "outcome": "failed" },
"deferred": ["default/2bb_-ILTnSIXmxGcZ2reEw: the document at [my-index-2] is index
              [-u4CHvjHVw70SKHigBad0A], so the assigned index is not present yet"]
```

The assignment still named the **old** uuid after 615 passes. That is correct behaviour by the data plane — a
document under a reused name describing a different index is exactly the torn read `NODE_CONTRACT.md` says to
decline — and it means the loop is stuck on this side. Two separate gaps produce it.

### 7a. `ORPHANED_COPY` is raised and consumed by nothing

`Differ` raises it for every copy a node holds whose index is absent from desired state, with a message that ends
*"nothing acts on it until it has held"*. Nothing acts on it after it has held either: `grep ORPHANED_COPY` finds
the enum, the `Differ`, and no planner or applier code at all.

So a copy of a deleted index is held for ever, and the `DELETE_INDEX` task correctly refuses to finish —
*"given up after 5 attempts; last verdict: node [X] still holds or is assigned a copy of index uuid Y"*. Six
`TASK_STUCK` findings in the store are that, accumulated across runs.

**Related and separate:** even where a node holds nothing, an **assignment entry** for an index absent from desired
state is never pruned. The node declines it every pass, the assignment version never moves, and the entry outlives
the index indefinitely. A stale entry is not a copy, so `ORPHANED_COPY` does not cover it.

Suggested: plan a `RemoveCopy` for an `ORPHANED_COPY` that has held for `minimumPasses`, and prune assignment
entries whose index uuid is not in the tenant's desired state as a property of writing the assignment — the same
shape as the coordinator-membership fix, which is not finding-driven either.

### 7b. A primary cannot displace a replica on the last node

`placeable_nodes: 1`, and the one live node holds `pull-based-index`'s **search replica** while the node that held
its **primary** is among the 23 given up on. So:

```
UNASSIGNED_PRIMARY :: index [pull-based-index] shard 0 has no primary on any node in this pool
  unplaceable: [same_shard] node [ENQgP1TR...] already holds, is assigned, or is about to be given a copy
```

Correct by the rule and a permanent deadlock: the shard's only surviving copy is a read-only one, the only
candidate is the node holding it, and `SameShardDecider` refuses. Nothing will ever place that primary, and the
index cannot be written to again.

A primary and a replica competing for the last node is not a tie. Suggested: when the finding is
`UNASSIGNED_PRIMARY` and the refusal is `same_shard` against a node whose existing copy of that shard is a
*replica*, the replica should be given up — planned as a `RemoveCopy` in the same pass, or promoted if the
deployment ever allows a search replica to become a primary (upstream does not). Either beats a shard that can
never be written to.

### One thing this write-up does not explain — answered

> **There was no third thing.** The refusal was real and was not about that shard's primary. `Plan.refusals` was
> keyed by *subject*, and all three unassigned kinds of one shard share a subject — so on a one-node pool the
> primary is placed, the search replica is then correctly refused because the only node has just been given the
> primary, and the refusal is rendered against the primary's finding. Reproduced, and the failure message is the
> same sentence quoted below, attributed to `UNASSIGNED_PRIMARY`. Fixed: `Finding.refusalKey()` includes the kind
> and `Plan.refusalFor(finding)` is the only way to read one.
>
> **A second, separate deadlock was found while looking.** `Differ` counted a copy only when a node reported one;
> the planner also counted a copy a node had merely been *assigned*. A node handed a primary that died before
> reporting it satisfied the planner and not the differ, so the shard was announced unassigned every pass and
> skipped before the allocator ran — no placement and no refusal, which matches "the stored documents do not
> support that" better than anything in this section. Fixed by `NodeObservation.actsOnItsAssignment()`, which
> requires a heartbeat: a node that has gone quiet keeps its claim through the stale window, for GC tolerance,
> but a node that never spoke does not.
>
> **7a and 7b are not fixed** — see the notes at the end of this item.

### One thing this write-up does not explain (original text)

The same pass also refused `my-index-2`'s primary with `same_shard` on that node, and the stored documents do not
support that: the node neither holds nor is assigned the new uuid, and `heldCopy` correctly keys on
`index.uuid()`. Ordering is not it either — primaries are added to `actionable` before replicas and search
replicas. So there is a third thing happening that these artifacts do not show, and it wants instrumenting rather
than guessing at: the candidate list, `nodesWithThisShard()` and the `planned` list for that finding, in one pass.

### The endpoint collision, from the status output

> The second deferred line — `[127.0.0.1:9300] is already claimed by node [...]` — was a supervisor bug and the
> message's own diagnosis was correct. Two things, both in `DESIGN.md` §20.
>
> **Fixed:** the projection now publishes **one node per `address:port`**, the freshest of them. A process owns a
> socket, so two nodes on one endpoint means one is history, and only the supervisor can tell — a node sees its own
> registration and no other. Keyed on address *and* port, so several nodes on one laptop keep working.
>
> **Not fixed, and the reason is worth knowing:** nothing reaps a departed node's documents, and three of the four
> are not the supervisor's to delete. A node partitioned for longer than any grace period is indistinguishable from
> a dead one, and deleting its registration while it is merely unreachable leaves it heartbeating with no
> registration — `NODE_UNREGISTERED`, placeable for nothing, needing a restart. **The clean fix is a contract rule
> that a node rewrites its registration when it notices it missing**, which makes reaping safe by construction and
> is yours as much as mine. §20.2 has the alternative.

### On 7a and 7b, from this side

**7b is right and worth doing**, and it is the one that definitely stops an index being written to. Not attempted
here: displacing a replica to make room for a primary means planning a `RemoveCopy` against a copy that is
currently serving, which is the first time this service would delete a *working* copy in order to place another,
and it wants its own thinking about ordering — the primary must be planned in the same pass or the shard has no
copies at all for a window.

**7a's second half is not safe as suggested.** Pruning assignment entries whose index uuid is absent from desired
state "as a property of writing the assignment" has no patience in it, and an absent index document is exactly the
torn read `R2.5` promises will happen — the index may simply not have landed yet. `ORPHANED_COPY` waits
`minimumPasses` for that reason, and the applier has no finding-age memory to wait with. So this needs either a
finding that ages (a new kind covering a stale *entry*, which is not a copy) or the applier gaining access to the
ages. The first half — planning a `RemoveCopy` for an aged `ORPHANED_COPY` — has the patience already and is the
cheaper start.

### Reproducing it

Delete an index and recreate it with the same name, then watch a data node's `/_clusterless/status`: the deferred
line names the old uuid against the new document, and `assignment_version` stops moving. It survives restarts of
the supervisor, which is what makes it a state problem rather than a timing one.

---

## 8. A consumer-pointer reset arrives after the resume that makes it illegal

**Not a defect here — an answer to `DESIGN.md` §21.2, and one thing in it that will not work as sequenced.** The
pause half (§21.1) is done on this side: `IndexConfig` reads `ingestion_status.is_paused` and both fabricators put
it into `IndexMetadata` via `IngestionStatus`, so a pause reaches a running poller level-triggered with no new
plumbing. Verified by reading the server: `IndicesClusterStateService.createOrUpdateShards` calls `updateShard` for
every shard on the node on **every** applied state with no equality guard, and that calls
`IndexShard.updateShardIngestionState(IndexMetadata)`. Nothing latches, and `resume` is as ordinary as `pause`.

### The agreement first, because it is the bigger half

**Requiring an acknowledgement rather than detecting the effect is right**, and the argument for it is the one that
matters: rewinding a consumer from 5000 to 1000 looks, *before* it is applied, exactly like a job already done. Any
completion rule that reads the pointer drops that instruction before anything acts on it. Both obligations §21.2
puts on this side are accepted:

- `applied: applied` will mean the reset too, or the acknowledgement means nothing;
- idempotence keys on the reset's `id`, not on the assignment version, because the version moves for unrelated
  reasons and re-applying a reset is the rewind-for-ever failure arriving by another route.

### What will not work: the pause and the reset arrive by different channels, and the fast one is the wrong one

Upstream refuses a reset on a poller that is running:

```java
// IngestionEngine.resetStreamPoller
if (streamPoller.isPaused() == false) {
    throw new IllegalStateException("Cannot reset consumer when poller is not paused");
}
```

And the control plane writes the resume **before** it records the task carrying the resets —
`IngestionHandlers.setPaused` sets `ingestion_status.is_paused` to `false`, `indices.replace(...)` it, and only then
`tasks.submit(TYPE_RESUME_INGESTION, …, details)`. Those two facts compose badly, because the two halves of one
operator action reach the node down different paths of different lengths:

- `is_paused: false` is **desired state** in a document this node already reads every pass. It converges on the
  next pass, unconditionally.
- the reset is an **instruction** that has to be observed by the supervisor, planned into an assignment, written,
  and read. Several passes, at best.

So the ordinary case is that the node resumes the poller from the index document, and the reset arrives afterwards
and throws. The window is not a race to be narrowed — it is the normal ordering.

### The fix is on this side, and it is cheap, but the contract has to say so

The node must not depend on the poller's current state. `IngestionSettings` is a partial update — every field
nullable — and `IngestionEngine.updateIngestionSettings` applies the reset *first* and the pause flag second, so
one call carrying both cannot help: the reset still runs against a live poller. Two calls to
`IndexShard.updateShardIngestionState(IngestionSettings)`, which is local and needs no cluster manager:

1. `setIsPaused(true)` — stop the poller whatever it was doing;
2. `setResetState(...).setResetValue(...).setIsPaused(config.ingestionPaused())` — reset, then land on the pause
   state the index document actually asks for.

That is correct for a poller that was already paused, one that was resumed a pass ago, and one that was never
paused at all. It costs a brief stop on a stream that was running, which is inherent to resetting a consumer
rather than something this sequencing adds.

**So the obligation is mine, and what I need from the contract is only that it be written down** — that a node
receiving `ingestion_reset` pauses before applying it rather than assuming the document's pause has been applied
first. A node that trusted the ordering would work in every test where the reset happened to arrive first.

### Two things to pin in the contract while it is open

- **The mode spelling, in one place.** The control plane validates `OFFSET` and `TIMESTAMP` on the wire; upstream's
  enum is `StreamPoller.ResetState.RESET_BY_OFFSET` and `RESET_BY_TIMESTAMP`; the value is always a string, parsed
  as millis for a timestamp. Three repositories are currently free to guess at the middle spelling. The sketch's
  `{"mode": "offset", "value": "1000"}` is fine — say that it is the wire spelling lowercased and that the node
  maps it.
- **Completion when the resets span nodes.** A reset is per shard and an assignment is per node, so a task naming
  shards 0 and 1 whose primaries sit on different nodes needs *both* acknowledgements. And a shard whose primary is
  unassigned cannot acknowledge at all, so the task must stay incomplete with a reason rather than complete or
  fail — the same patience as everything else here.

### Why no memory is needed anywhere, which is worth stating in §21.2

`resetStreamPoller` ends with `flush(true, true)` — "force flush to persist the new batch start pointer" — so a
reset lands in the Lucene commit. This plugin's recovery already prefers local bytes and reads
`StreamPoller.BATCH_START` out of the commit user data (M8.2), so a shard that moves *after* a reset resumes from
the reset point without the instruction existing anywhere any more. The reset survives through the commit rather
than through the assignment, which is exactly why the supervisor can stop writing it the moment it is acknowledged
and why nothing needs to remember having applied it.

### Tests worth having, on your side

- a `resume-ingestion` task carrying resets writes `ingestion_reset` into the assignment of the node holding each
  named shard's **primary**, and not into any other node's;
- the task completes only when every named shard's holder reports `applied` at the assignment version now in the
  store, and stays incomplete — with a reason — when one of them has no primary;
- the reset stops being written once the task completes, and re-observing the same state does not re-plan it;
- an assignment version that moves for an unrelated reason does not change the reset's `id`.

---

## Where these came from

The same-host relaxation already committed here (`DESIGN.md` §2, *A failure domain is what an operator says it
is*) is the third finding from the same run, and it is the one that was actually blocking. These two are what made
it take an afternoon rather than a minute: the index reported green while a copy was missing, and when it stopped
reporting green the explanation named the wrong node.
