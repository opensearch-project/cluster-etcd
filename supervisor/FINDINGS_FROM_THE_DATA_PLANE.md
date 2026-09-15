# Findings from the data plane

Four changes wanted in this repository, all found by running the real data plane
(`../opensearch-data-plane`) against a real supervisor, a real control plane and one store. None loses data.

The first two are cases where **the supervisor made a decision and did not say so** — the failure mode this
design spends most of its effort trying to avoid — so they are worth closing properly rather than patching. The
third was a missing field that made a projection unable to serve either of the two readers it was written for; it
is **done** as of `50ecdf5`, and reading it is still worth it for why an address was not sufficient. The fourth
blocks the milestone the third unblocked: nothing ever assigns the coordinator role, so the routing this enabled
cannot be exercised.

Each item says what happened, why the current behaviour is wrong, where the code is, what a fix has to be careful
about, and the tests worth having. All three are self-contained.

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

### Tests worth having

- a pool with one coordinator-roled node and two bound tenants writes an assignment naming both, with no indices;
- a tenant rebound away has its coordinator entry removed on the next pass;
- a coordinator entry is written even in a pass whose work ceiling was exhausted by placements;
- whichever answer item 4's second half gets, a node that both holds copies of an index and coordinates for its
  tenant can be told so, and the data plane's reader agrees about what the document means.

---

## Where these came from

The same-host relaxation already committed here (`DESIGN.md` §2, *A failure domain is what an operator says it
is*) is the third finding from the same run, and it is the one that was actually blocking. These two are what made
it take an afternoon rather than a minute: the index reported green while a copy was missing, and when it stopped
reporting green the explanation named the wrong node.
