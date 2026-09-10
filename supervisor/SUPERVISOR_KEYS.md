# What the supervisor writes

Every key the supervisor owns, and who reads it. Peer to [`KEY_LAYOUT.md`](KEY_LAYOUT.md) (the control
plane's keys) and [`NODE_CONTRACT.md`](NODE_CONTRACT.md) (the data plane seam). This file covers everything
else: the projections the control plane reads, the supervisor's own bookkeeping, and the observability
surface.

Like `NODE_CONTRACT.md`, this is a proposal. Nothing here exists yet.

## R5.5 cannot be read literally, and here is why

> **R5.5** The supervisor must not write into the control plane's key space. Its own prefixes only.

The intent is right and the literal reading is impossible. The control plane's handlers do not see the root
store — they see a `TenantScopedKeyValueStore` rooted at `tenant/<id>/`, so **a key outside that prefix is
not merely undiscovered by the control plane, it is unreachable.** Health for `cluster.health`, node and
shard state for `cluster.state`, snapshot metadata for `snapshot.get`, discovered awareness values for
weighted routing validation: R5.1–R5.4 all require the control plane to read something the supervisor wrote,
and all of it must therefore live under `tenant/<id>/`.

So the rule is kept, one level down:

> **`tenant/<t>/status/**` is written by the supervisor and read by the control plane. Everything else under
> `tenant/<t>/` is written by the control plane and read by the supervisor. One segment decides the writer.**

`status/` is reserved. The control plane's top-level segments under a tenant today are `indices`, `aliases`,
`data_streams`, `templates`, `ingest`, `search`, `scripts`, `snapshot`, `cluster` and `tasks`; `status` is
free and must stay that way. Adding a sibling is a change to both services, which is what R5.5 was actually
protecting.

Note that health does **not** go at `tenant/<t>/cluster/health`, next to `cluster/settings` and
`cluster/uuid`, even though that reads better. Those are control plane keys and ownership has to be
decidable from the key alone.

## The whole key space, one table

| key | writer | reader | requirement |
|---|---|---|---|
| `tenant/<t>/indices/<name>` etc. | control plane | supervisor | `KEY_LAYOUT.md` |
| `tenant/<t>/tasks/index-management/<uuid>` | **both** — see below | both | R4 |
| `tenant/<t>/status/cluster` | supervisor | control plane | R5.1, R5.2 |
| `tenant/<t>/status/indices/<index-uuid>` | supervisor | control plane, **coordinators** | R5.1, R5.2 |
| `tenant/<t>/status/nodes/<node-id>` | supervisor | control plane | R5.2, R5.4 |
| `tenant/<t>/status/awareness` | supervisor | control plane | R5.4 |
| `tenant/<t>/status/snapshots/<repo>/<snapshot>` | supervisor | control plane | R5.3 |
| `nodes/registration\|heartbeat\|shards/<node-id>` | data plane | supervisor | `NODE_CONTRACT.md` |
| `nodes/assignment/<node-id>` | supervisor | that node | `NODE_CONTRACT.md` |
| `pools/definitions/<pool-id>` | operator | supervisors | `DESIGN.md` §2 |
| `pools/bindings/<tenant-id>` | operator, **and the outgoing owner on a rebind** | supervisors | `DESIGN.md` §2, §4 |
| `pools/nodes/<node-id>` | operator | supervisors | `DESIGN.md` §2 |
| `supervisor/members/<supervisor-id>` | that supervisor | supervisors, operators | R6.1 |
| `supervisor/pools/<pool-id>/lease` | supervisors | supervisors, operators | R6.1, R6.2 |
| `supervisor/leases/housekeeping` | supervisors | supervisors | `DESIGN.md` §3 |
| `supervisor/status/pools/<pool-id>/convergence` | that pool's owner | operators | R7.2 |
| `supervisor/status/pools/<pool-id>/oscillation` | that pool's owner | operators | R7.3 |
| `supervisor/status/diff/<tenant-id>` | that tenant's pool owner | operators | R7.1 |
| `supervisor/status/members/<supervisor-id>` | that supervisor | operators | R3.3 |
| `supervisor/status/housekeeping` | the housekeeping holder | operators | `DESIGN.md` §3 |
| `tenants/<tenant-id>` | **control plane — proposed** | supervisors | see §6 |

Every key above has **exactly one writer at any moment**, and `DESIGN.md` §2 shows why that falls out of the
pool invariants rather than being maintained by care. The two rows that need reading twice are
`pools/bindings/<tenant-id>`, which an operator writes to start a rebind and the outgoing pool owner writes
to finish one, and `supervisor/pools/<pool-id>/lease`, which is the single place in the system where two
supervisors deliberately race.

## 1. Projections for the control plane

Each unblocks an operation that returns 501 today. They are worth doing in the order below, because each is
independently shippable.

### `tenant/<t>/status/indices/<index-uuid>` — the load-bearing one

One document per index, carrying health **and** routing, because they are computed from the same
observation and consumed together. It serves **four** readers at once: `cluster.health` per index,
`cluster.state`, coordinator routing, and — under document replication — a data node resolving the peers it
must replicate to or from. `NODE_CONTRACT.md` sends all of them here rather than duplicating a routing table
into every assignment, which would store it once per reader and rewrite every copy whenever one shard moved.

```json
{
  "index": "my-index",
  "index_uuid": "uzKxLWZu4nPSrTknEcDpoA",
  "config_revision": 3041,
  "status": "yellow",
  "number_of_shards": 2,
  "number_of_replicas": 1,
  "active_shards": 3,
  "active_primary_shards": 2,
  "unassigned_shards": 1,
  "initializing_shards": 0,
  "replication": "DOCUMENT",
  "shards": {
    "0": [ { "node_id": "pv1sDM…", "role": "PRIMARY", "state": "STARTED" },
           { "node_id": "72ZSKe…", "role": "REPLICA", "state": "INITIALIZING" } ],
    "1": [ { "node_id": "zF5QLA…", "role": "PRIMARY", "state": "STARTED" } ]
  },
  "observed_at": 1788969977933,
  "observed_pass": 10432
}
```

- **Keyed by uuid, not name** — the same reason `NODE_CONTRACT.md` is. A name is reused; a control plane
  reading by name resolves the name to a uuid through the index document it already has.
- **`status`** is `green` / `yellow` / `red` on the OpenSearch definition: red if any primary is missing,
  yellow if any replica is, green otherwise. A **closed** index (`state: "close"` in the index document) is
  reported `green` with no shards, matching OpenSearch — a closed index is not unhealthy.
- **`config_revision`** is the revision of the index document this was computed from, so a reader can tell
  whether the projection has caught up with a settings change.
- **`observed_at` and `observed_pass`** make staleness visible. A projection that stops updating is the
  failure mode this whole file has to make detectable — see §4.
- **Every observed copy is listed, including copies that are not yet serving**, with `role` and `state`
  separately. Not an accident of the encoding: a docrep replica reads this document to find the primary it
  must replicate from, and a projection filtered to `STARTED` copies would deadlock — the replica could
  never see the primary it is waiting for, because the primary is not serving until the replica catches up.
  `role` and `state` are separate for the reason `NODE_CONTRACT.md` gives: a primary that is still
  initializing is a thing that exists, and `active_primary_shards` cannot be computed without saying so.
- **`replication`** is `DOCUMENT` or `SEGMENT`, copied from the index's `index.replication.type`. It tells a
  reader whether peer resolution applies at all before it goes looking.
- **Size** is bounded by shard count × copies, at roughly 60 bytes each. An index of 1000 shards with two
  copies is ~120 KiB, under the 512 KiB budget. An index that would exceed it is a finding, not a truncated
  write.

### `tenant/<t>/status/cluster` — `cluster.health` and the node list

```json
{
  "status": "yellow",
  "number_of_nodes": 12,
  "number_of_data_nodes": 9,
  "active_primary_shards": 240,
  "active_shards": 465,
  "unassigned_shards": 15,
  "initializing_shards": 3,
  "delayed_unassigned_shards": 0,
  "indices_by_status": { "green": 78, "yellow": 3, "red": 0 },
  "observed_at": 1788969977933,
  "observed_pass": 10432
}
```

Cluster status is the worst index status. Deliberately **no per-index map here** — `cluster.health?level=indices`
composes it from the per-index documents, so this stays a small, fixed-size document that can be rewritten
every pass. Putting a map of every index in the hot document is the same mistake the node heartbeat avoids.

### `tenant/<t>/status/nodes/<node-id>` — for `cluster.state`, and R5.4

A tenant-scoped view of a node in the tenant's pool: only nodes serving this tenant appear, and only the
slow-changing facts. **No load or capacity figures**, so that a node's disk filling up does not rewrite the
document — that data is already in `nodes/heartbeat/<id>` for anyone entitled to read the root.

```json
{
  "node_id": "pv1sDMfZRxSTMiW5VrbN9w",
  "name": "data-3",
  "roles": ["data"],
  "address": "10.0.3.14",
  "port": 9300,
  "version": "3.7.0",
  "attributes": { "zone": "z1", "rack": "r7" },
  "liveness": "live",
  "observed_at": 1788969977933
}
```

`liveness` is `live`, `stale` or `lost`, on the two-threshold rule in `NODE_CONTRACT.md`.

### `tenant/<t>/status/awareness` — the check the control plane cannot make today

Derivable from the node documents, and kept anyway: it is one tiny document, and it saves the control plane
a prefix scan on the validation path of every weighted-routing request.

```json
{
  "attributes": { "zone": ["z1", "z2", "z3"], "rack": ["r1", "r7"] },
  "observed_at": 1788969977933
}
```

Values are the distinct values across nodes currently serving the tenant. **A value disappears only when no
live node reports it**, and that is a hazard worth stating: a zone whose nodes are all down would silently
stop being a valid weighted-routing target. Include values from `stale` nodes, exclude `lost` ones.

### `tenant/<t>/status/snapshots/<repo>/<snapshot>` — R5.3

Projected from repository reads so `snapshot.get` and `snapshot.status` can be served without the control
plane growing a second outbound seam, which `REQUIREMENTS.md` records as an explicit scope decision. Shape
deferred: it depends on repository access, which is the furthest-out piece of this service and the only
part that talks to something other than the store.

## 2. The task lifecycle, and the second place R5.5 gives way

`tasks/index-management/<uuid>` is a control plane key. R4.3 requires the supervisor to move tasks to a
terminal state and delete them. Those cannot both hold.

`KEY_LAYOUT.md` settles it — *"completing and reaping these tasks is the supervisor's half of the handoff,
and it is the single most load-bearing sentence in this document"*. So the task subtree is **jointly owned**,
with a protocol rather than an owner:

| actor | may |
|---|---|
| control plane | create a record; read any record |
| supervisor | update `state`, `attempts`, `holder`, `lease_expires_at`; delete a terminal record |
| control plane | **never** update or delete |

A claimed record:

```json
{
  "task_id": "NmnqMpOCtk1I5aW_zVzSkA",
  "type": "create-index",
  "index": ".ds-logs-app-000001",
  "index_uuid": "JqyK2fDEK5RrEZWGo4SPDA",
  "state": "claimed",
  "submitted_at": 1788969977936,
  "sequence": 2,
  "holder": "supervisor-a1b2",
  "epoch": 7,
  "lease_expires_at": 1788969997936,
  "attempts": 1,
  "last_error": null
}
```

- **`state`** becomes `claimed`, then `completed` or `failed`. The control plane's `cluster.pending_tasks`
  reports only `pending`, so the pile starts draining the moment claiming works — before completion is even
  implemented.
- **Claiming is a compare-and-set on the record's own revision** (R4.1, R6.2), not a lock. A holder that
  dies leaves an expired `lease_expires_at`, and any supervisor may then re-claim by CAS. The loser of a
  race gets a `RevisionConflictException` and moves on.
- **`attempts` increments on each claim.** Past a limit (default 5) the task goes to `failed` with
  `last_error` and is **not** reaped — R4.2 wants failure visible, and a deleted failure is not.
- **`completed` records are deleted** after a retention window (default 1 hour), which is the only reason
  they are written at all rather than deleted outright: an operator needs a moment to see that a task
  finished.
- **`sequence` and `submitted_at` are not used for ordering** (R4.5). Where the supervisor needs a total
  order it sorts by `(submitted_at, task_id)` and treats that as arbitrary-but-stable, which is enough for
  fairness and not enough for causality — and it never needs causality, because tasks are hints.
- **Nothing above is required for correctness.** With the task subtree empty the supervisor converges from
  the `indices/` documents alone (R2.1). This section exists to drain a queue and to make failures visible,
  not to drive reconciliation.

### What "complete" means, and the definition this is not

**Complete convergence for the task in hand.** A `create-index` task is done when every shard of the index
has a serving primary; a `delete-index` task when the index document is gone *and* no copy of it remains
anywhere. Simple, unambiguous, consistent with OpenSearch's existing behaviour, and achievable at the scale
this is built for.

It is worth writing down what it is not, because the alternative is better and harder.

**The alternative is functional completeness**, and the argument for it is a scale argument. At cloud scale
you essentially never have 100% of what you asked for, and that is fine — with redundancy across data
centres a service is fully functional at some well under 100%. A definition of "complete" that requires
everything therefore becomes a definition that is rarely satisfied, and a queue whose entries mostly sit
open is not much better than one that never drains.

Functional completeness would ask a different question: *is the thing usable?*

| task | complete convergence (implemented) | functionally complete |
|---|---|---|
| `create-index` | every shard has a serving primary | at least one copy of each shard on a data node **and** a coordinator routing to them |
| `delete-index` | document gone and no copies remain anywhere | removed from coordinator routing — nobody can reach it, whether or not the bytes are still on disk |

**Why it is deferred rather than adopted.** The functional definition is genuinely per task type — the two
rows above have nothing structural in common, and each new task type would need its own judgement about what
"usable" means for it. That is a real cost against a benefit this deployment does not need yet: complete
convergence is reachable at the scale this will run at for the foreseeable future. Recorded so that the day
it stops being reachable, the reason it was chosen is on the record along with what to do instead.

Two smaller notes on the implemented definition, both places where it is deliberately not the strongest
reading available:

- **Replicas are excluded from `create-index`.** Requiring every desired copy would be more obviously
  correct, but the planner cannot place replicas yet, so every such task would fail — blaming the task for an
  unimplemented feature. Missing replicas are reported as `UNASSIGNED_REPLICA` regardless. This is the line
  to move when replica placement lands.
- **A task whose subject no longer exists is complete, not failed.** A `create-index` task for an index since
  deleted describes work nobody wants. Failing it would leave a permanent record of something that never went
  wrong.

## 3. Pools, members and leases

`DESIGN.md` §2 and §3 explain why these exist and what they buy. This is their shape.

### `pools/definitions/<pool-id>` — operator-written

```json
{
  "pool": "shared-a",
  "kind": "shared",
  "default": true,
  "description": "long tail, us-west-2",
  "max_shard_copies_per_node": 2000
}
```

`kind` is `shared` or `dedicated` and is documentation plus a guard: binding a second tenant to a
`dedicated` pool is refused and raises a finding rather than silently making it shared. **At most one pool
may set `"default": true`**, and a second one is a finding — two defaults would make the destination of an
unbound tenant depend on scan order.

### `pools/bindings/<tenant-id>` — one key per tenant, and that is the point

```json
{ "pool": "shared-a", "generation": 4 }
```

Mid-rebind (`DESIGN.md` §4):

```json
{ "pool": "shared-a", "moving_to": "dedicated-acme", "generation": 4 }
```

One key per tenant so that **rebinding is a single compare-and-set**. Holding the membership list inside
the pool document instead would make a move two writes to two keys with no transaction, and the torn state
is "this tenant is in both pools" — two supervisors placing its shards, which is the one thing the whole
partition model exists to prevent.

`generation` increments on every change and is stamped into the assignments written under that binding, so
an assignment written by the previous owner is identifiable after the fact.

### `pools/nodes/<node-id>` — the override

```json
{ "pool": "dedicated-acme" }
```

Optional. A node's pool normally comes from the `pool` field of its own registration document, which it
takes from its deployment configuration, so a node deployed into a pool needs no write here at all. This key
**overrides** that, which is how a node moves between pools without being redeployed — one key, one CAS, and
its shard copies move with it. Precedence is stated once, in `NODE_CONTRACT.md`, and both sides must agree.

### `supervisor/members/<supervisor-id>`

```json
{
  "supervisor_id": "supervisor-a1b2",
  "epoch": 7,
  "started_at": 1788969000000,
  "expires_at": 1788969930000,
  "pass": 10432,
  "capacity_weight": 1.0,
  "pools_held": ["shared-a", "dedicated-acme"],
  "weight_held": 340.0
}
```

Renewed every pass. An expired record is treated as dead: its capacity leaves the fair-share denominator
and it is reaped by the housekeeping holder after a long grace period. `capacity_weight` is configured — it
is how a supervisor on a bigger machine is given more pools without a measurement loop that would have to be
trusted.

### `supervisor/pools/<pool-id>/lease` — the only lock

```json
{ "holder": "supervisor-a1b2", "epoch": 7, "acquired_at": 1788969900000, "expires_at": 1788969930000 }
```

Acquired and renewed by compare-and-set on the key's revision (R6.2). `epoch` increments on each acquisition
and is stamped into every assignment document the holder writes, so a split brain is diagnosable after the
fact from data the system already wrote.

**This is the entire mutual-exclusion story.** There is no global lock, no consensus, no leader-of-leaders.
Every other key in this file has one writer by construction (`DESIGN.md` §2), so this is the only place two
supervisors ever contend, and in steady state they do not.

`supervisor/leases/housekeeping` has the same shape and covers the chores that belong to no pool: unbound
tenants, unpooled nodes, unowned pools, and reaping long-dead member records.

## 4. Observability, projected rather than served

R7.1 wants an operator to be able to ask "what does this think is wrong right now?". Serving that over HTTP
would make the supervisor a second API service, which `REQUIREMENTS.md` §9 rules out, and would be
unreachable in exactly the
situation where it is wanted — a wedged process. So it is projected into the store, where it is durable,
inspectable with the same tooling as everything else, and readable when the supervisor is not.

The cost is that every one of these documents must be **bounded**, and a bounded diff is a truncated diff.
Each carries exact counts alongside the truncated list, so the summary is never wrong even when the detail
is incomplete.

**Scoping note.** Pool-scoped documents live under `supervisor/status/pools/<pool-id>/`; tenant-scoped ones
stay at `supervisor/status/diff/<tenant-id>`, keyed by tenant rather than by pool **on purpose** — a rebind
(`DESIGN.md` §4) changes a tenant's pool, and an operator's bookmark for "what is wrong with acme" should
not move when it does.

### `supervisor/status/pools/<pool-id>/convergence` — R7.2

```json
{
  "pool": "shared-a", "epoch": 7, "holder": "supervisor-a1b2",
  "pass": 10432,
  "pass_started_at": 1788969977800, "pass_duration_millis": 240,
  "weight": 340.0, "nodes": 40, "indices": 3000,
  "findings_total": 12,
  "findings_by_kind": { "UNASSIGNED_REPLICA": 9, "NODE_STALE": 1, "ORPHANED_COPY": 2 },
  "last_empty_diff_at": 1788969012000, "passes_since_empty": 300,
  "trend": { "window": 20, "delta": -3 },
  "work_ceiling_hit": false,
  "tenants": { "acme": { "findings": 11, "passes_since_empty": 300 },
               "globex": { "findings": 1, "passes_since_empty": 4 } },
  "watch_poll_failures": 0,
  "etcd_watch_failures": 0
}
```

- **`trend`** is what turns "12 findings" into an answer. Twelve findings shrinking is a recovery; twelve
  findings flat is the alarm, and `REQUIREMENTS.md` says so directly: *"A diff that is stable and non-empty
  is the alarm."*
- **`weight`** is what the balancer reads from other supervisors (`DESIGN.md` §3), so this document is on
  the coordination path and not only the operator path. A pool nobody owns has no current weight, which is
  why the balancer falls back to a configured default for unclaimed pools.
- **`pass_duration_millis` against the pass interval is the capacity signal.** A pool outgrowing its
  supervisor shows up here first, and `DESIGN.md` §2 has no other early warning to offer.
- **`watch_poll_failures`** is `Watches.pollFailures()` and **`etcd_watch_failures`** is
  `EtcdKeyValueStore.watchFailures()`. `WORKING_NOTES.md` and §8 both insist these be published, for a good
  reason: a silently dead watch looks exactly like a quiet keyspace. On the remote backends the poll counter
  stays zero by design, which is itself worth knowing when reading this document.
- **`tenants`** is capped at the 20 worst by finding count, with the rest summarised — it must not grow with
  tenant count, and a shared pool is where that matters.

### `supervisor/status/diff/<tenant-id>` — R7.1

```json
{
  "tenant": "acme", "pool": "shared-a", "pass": 10432, "observed_at": 1788969977800,
  "counts": { "UNASSIGNED_REPLICA": 9, "ORPHANED_COPY": 2 },
  "truncated": false, "shown": 11, "total": 11,
  "findings": [
    { "kind": "UNASSIGNED_REPLICA", "index_uuid": "uzKx…", "index": "my-index", "shard": 1,
      "first_observed_pass": 10132, "passes": 300,
      "because": "no live node in pool shared-a satisfies awareness attribute zone=z3; z3 has 0 live nodes" }
  ]
}
```

**`because` is the field this whole document exists for** — R7.4 asks for the decision, not the action.
*"Moving shard X to node Y because node Z has been silent for 40s"* is debuggable; the same sentence without
its clause is not. A finding without a `because` should not pass review. Note that the reason now has to
name the **pool**, because "no node satisfies this" is only ever true within a partition.

**`first_observed_pass` and `passes`** are how expected torn state is told apart from a real problem
(R2.5). An index without its alias appearing for one pass is normal and must not raise an alarm; the same
finding at 300 passes is a stuck reconciler. The alarm is on **age**, never on presence.

Findings are ordered by severity then age, so truncation drops the least interesting.

### `supervisor/status/pools/<pool-id>/oscillation` — R7.3

A bounded ring of subjects that keep reappearing in the diff while the supervisor keeps acting on them —
*"the characteristic failure of this kind of service"*. Detection: the same subject receiving ≥ K actions
(default 5) within a window (default 50 passes) without its finding clearing, or alternating between two
finding kinds.

```json
{
  "pool": "shared-a", "pass": 10432,
  "detected": [
    { "subject": "acme/uzKx…/1", "kinds": ["UNASSIGNED_REPLICA", "SURPLUS_COPY"],
      "actions": 14, "window_passes": 50, "first_detected_pass": 10380,
      "because": "placed on node A (least disk used), removed as surplus, replaced on node B, repeat" }
  ]
}
```

Held in memory and projected. Losing it on restart is acceptable: it re-detects within one window, and an
oscillation that has stopped is not worth persisting.

**Pool churn is the second thing to watch here.** A pool changing hands repeatedly is the balancer
oscillating rather than the allocator, and it is recorded in the same document with `subject: "pool"` —
same detector, same shape, because it is the same failure one layer up.

### `supervisor/status/members/<supervisor-id>` — R3.3

The **effective** configuration of one supervisor process, published because R3.3 requires the liveness
threshold's effective value to be observable, and because "what is this process actually running with" is
the first question of every incident. Per member rather than global, since two supervisors can be running
different builds with different settings — which is itself worth being able to see.

```json
{
  "supervisor_id": "supervisor-a1b2", "epoch": 7, "started_at": 1788969000000,
  "version": "0.1.0-SNAPSHOT",
  "store_backend": "etcd",
  "pools_held": ["shared-a", "dedicated-acme"],
  "liveness": { "multiplier": 6, "floor_millis": 20000, "lost_multiplier": 20,
                "effective_millis": { "pv1sDM…": 30000, "72ZSKe…": 30000 } },
  "pass_interval_millis": 2000,
  "lease_ttl_millis": 15000,
  "work_ceiling_per_pass": 200,
  "balancer": { "hysteresis": 0.25, "release_after_passes": 20, "capacity_weight": 1.0 },
  "task_max_attempts": 5,
  "orphan_confirm_passes": 3
}
```

`effective_millis` is capped at the 50 nodes with the most unusual values; a 200-node cluster must not
produce a 200-entry document here.

### `supervisor/status/housekeeping`

Written by the holder of `supervisor/leases/housekeeping`. Everything that belongs to no pool, and every one
of these is a configuration mistake that would otherwise be silent:

```json
{
  "pass": 10432, "holder": "supervisor-c3d4", "observed_at": 1788969977900,
  "unpooled_tenants": ["newco"],
  "unpooled_nodes": ["aB3xQ…"],
  "unowned_pools": [],
  "pools_without_definition": ["typo-pool"],
  "members_live": 3, "members_expired_reaped": 1,
  "registry_source": "tenants/"
}
```

`unpooled_tenants` is the one that matters. A tenant the control plane is serving, with indices, that no
supervisor is managing, produces no error anywhere else in the system — no shards are placed, no health is
computed, no tasks are reaped, and every one of those looks like idleness. This list is the only place it
shows up, which is why §6 cares so much about the tenant registry existing.

## 5. Size budgets, collected

Both backends cap value size — ZooKeeper at 1 MiB by default, etcd at 1.5 MiB — and **no document may be
truncated to fit**, because a truncated document parses as a complete one and reads as "the missing things
are gone".

| document | budget | on exceeding |
|---|---|---|
| `nodes/heartbeat/<id>` | 4 KiB hard | cannot happen; nothing in it scales |
| `nodes/shards/<id>` | 512 KiB | node writes nothing, reports `outcome: failed` |
| `nodes/assignment/<id>` | 512 KiB | supervisor writes nothing, raises a finding |
| `tenant/<t>/status/indices/<uuid>` | 512 KiB | write nothing, raise a finding |
| `tenant/<t>/status/cluster` | 4 KiB | cannot happen; fixed shape |
| `pools/*`, `supervisor/members/*`, `supervisor/pools/*/lease` | 4 KiB | cannot happen; fixed shape |
| `supervisor/status/*` | 64 KiB | truncate the **list**, keep the counts exact |

The observability documents are the only ones allowed to truncate, and only because they carry exact counts
next to the truncated list. Everything else fails loudly.

Note what is *not* in this table: no document scales with the number of pools, supervisors or tenants in the
deployment. Every one of those is either a key per entity or a bounded top-N. That is what "horizontally
scalable" has to mean at the storage layer, and it is easier to hold to now than to retrofit.

## 6. Tenant discovery, and the change it needs from the control plane

The supervisor has to know which tenants exist, and **the store cannot list them.** `KeyValueStore` has
`get` and `scanPrefix` and no list-children or delimiter operation, so `scanPrefix("tenant/")` returns not
the tenant ids but every key of every tenant — the entire store, on every pass, to learn a list of names.

There are two lists involved, and conflating them is how a tenant goes silently unmanaged:

| list | source | answers |
|---|---|---|
| tenants that **exist** | `tenants/<id>`, written by the control plane | "who is the control plane serving?" |
| tenants that are **managed** | `pools/bindings/<tenant-id>`, written by an operator | "who has capacity assigned?" |

`scanPrefix("pools/bindings/")` is cheap and gives the second list, which is all a supervisor needs to do
its work. It is not enough to notice the failure that matters: **a tenant in the first list and not the
second is entirely unmanaged**, and nothing else in the system reports it — no shards placed, no health, no
tasks reaped, all of which look exactly like an idle tenant. Detecting that needs both lists, which is why
the registry is worth a change to another service rather than a configuration file here.

**Proposal handed over** at `../opensearch-control-plane/docs/proposed-tenant-registry.md`: one root key
`tenants/<tenant-id>`, written by `OperationContextFactory` on a tenant's first *write*, best-effort, never
deleted. The control plane already writes exactly this shape of key — the three `*-initialized` markers —
so the mechanism exists and the change is one call site.

Until it lands the supervisor falls back to a configured list of tenants, reads the registry when present,
and reports which source it used in `supervisor/status/housekeeping`. A fallback that does not say it is a
fallback is how this gets forgotten.
