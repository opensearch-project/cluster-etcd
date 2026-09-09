# Supervisor design

How the service works, as distinct from what it must do (`REQUIREMENTS.md`) and what it reads and writes
(`KEY_LAYOUT.md`, [`NODE_CONTRACT.md`](NODE_CONTRACT.md), [`SUPERVISOR_KEYS.md`](SUPERVISOR_KEYS.md)).

No code exists. This is the design to argue with before any is written.

## 0. Decisions taken, and what they cost

| decision | alternative rejected | what it costs |
|---|---|---|
| **Independent of `../cluster-etcd`** | port its controller onto `store-api` | rebuilding allocation, health and election; its prior art informs, its schema does not |
| **The node pool is the unit of ownership** | the tenant, or the whole cluster | a pool must fit inside one supervisor, and a tenant cannot span pools |
| **Horizontally scalable, one lease per pool** | a single leader | a balancer, member records, and a failover window |
| **Multi-tenant data model from day one** | single `default` tenant | every structure is tenant-keyed; tenant discovery needs a change to the control plane |
| **Observability projected into the store** | an HTTP endpoint | every observability document must be bounded, so the diff is truncated |
| **Read-only first** | act as soon as the diff works | a milestone that ships nothing user-visible, and is worth it |

## 1. Everything is a pure function except two edges

The whole service is:

```
      read                    pure                          write
  ┌──────────┐        ┌──────────────────────┐        ┌──────────┐
  │  store   │ ─────► │ Observation → Diff   │ ─────► │  store   │
  └──────────┘        │      Diff → Plan     │        └──────────┘
                      └──────────────────────┘
```

`WORKING_NOTES.md` asks for this directly — *"take observations as data, not as a store handle"* — and the
sandbox makes it more than a preference: with no sockets, no containers and no Gradle, the only code
testable here is the code that touches nothing. That happens to be the code carrying the risk.

`diff` and `plan` take records and return records. They do not see a `KeyValueStore`, cannot read a key they
forgot to observe, and cannot be non-deterministic. A test is two literals and an assertion.

```java
record Observation(
    long pass, Instant observedAt, PoolId pool,
    Map<TenantId, DesiredState> tenants,
    Map<NodeId, NodeObservation> nodes) {}

record DesiredState(
    Map<String, IndexConfig> indices,        // by name, as the store keys them
    Map<String, AliasConfig> aliases,
    Map<String, DataStreamConfig> dataStreams,
    List<TaskRecord> tasks) {}

record NodeObservation(
    Optional<Registration> registration,
    Optional<ShardInventory> inventory,
    Optional<Heartbeat> heartbeat,
    Liveness liveness,                       // LIVE | STALE | LOST | UNKNOWN
    Instant lastRevisionChange) {}

record Finding(
    FindingKind kind, TenantId tenant, Subject subject,
    Severity severity, String because,
    long firstObservedPass, long passes) {}

record Diff(long pass, PoolId pool, List<Finding> findings) {}
record Plan(long pass, Epoch epoch, List<Instruction> instructions) {}
```

**An `Observation` is scoped to one pool** — that is §2, and it is what makes the pure core scale: a
supervisor holding three pools runs three independent loops over three independent observations, sharing
nothing but a store connection.

`because` is a required constructor argument, not an optional field. R7.4 wants the reason recorded, and a
required argument is the only version of that which survives a deadline.

`Liveness` is computed at observation time, not inside `diff`, because it depends on the supervisor's own
history of when it last saw a revision change (`NODE_CONTRACT.md`). Keeping it out is what keeps `diff` pure.

## 2. Partitioning: the pool is the unit of ownership

This is the section the rest of the design hangs from.

**A pool is a named set of nodes together with the tenants bound to it.** Two shapes, both first-class:

| shape | serves | for |
|---|---|---|
| **dedicated** | one tenant, on nodes nothing else uses | large, long-running tenants that are their own problem |
| **shared** | many tenants, on a common set of nodes | the long tail of small tenants |

Three invariants:

- **P1 — every node belongs to exactly one pool.**
- **P2 — every managed tenant is bound to exactly one pool.**
- **P3 — exactly one supervisor owns a pool at a time.**

From which, by construction:

| key | its single writer | by |
|---|---|---|
| `nodes/assignment/<node-id>` | the owner of that node's pool | P1 + P3 |
| `tenant/<t>/status/**` | the owner of that tenant's pool | P2 + P3 |
| `tenant/<t>/tasks/index-management/<uuid>` | the owner of that tenant's pool | P2 + P3 |

**No key has two writers, so there is no coordination on the data path at all.** Not a lock, not a
transaction, not a compare-and-set against another supervisor. The only shared-state coordination in the
whole system is one lease per pool (§3), and in steady state it is uncontended.

### Why the pool, and not the tenant

Partitioning by tenant is the obvious move and it only works if tenants do not share nodes. The contended
resource is the **node**: its assignment document is a single key, and two supervisors placing shards for
two different tenants onto the same node would both have to write it, on every pass, forever.

Partitioning by pool works in both cases and collapses to per-tenant partitioning exactly where per-tenant
partitioning was valid — a dedicated pool *is* a tenant partition. The shared pool is what tenant-based
partitioning could not express.

### The configuration this does not support, stated plainly

**Many large tenants on one large shared pool.** It is the only shape that cannot be split: the tenants
cannot be separated because they share nodes, and the nodes cannot be separated because they share tenants,
so the pool is indivisible and must fit in one supervisor.

This is a **deliberate non-goal**, on the stated ground that the deployments this serves do not need it —
large tenants get dedicated pools; shared pools hold small tenants. Recording it matters because it is
invisible until the day a shared pool's tenants have grown, and by then the fix is a data migration (§4)
rather than a configuration change. The pool weight published each pass (§3) is the early warning.

### The capacity envelope

A pool must fit inside one supervisor. Per-pass cost is roughly `O(nodes + indices + shard copies)` for
observation, and observation dominates — diffing is cheap and acting is capped (§7).

The envelope is to be **measured, not assumed**; nothing here has run. The signals that say a pool is
outgrowing its supervisor are already in the observability surface: `pass_duration_millis` approaching the
pass interval, `work_ceiling_hit` staying true, and `passes_since_empty` growing while `trend` is flat.

When a pool does outgrow one supervisor, the fix is to split the pool, which means moving tenants (§4).
**A dedicated pool cannot be split at all**, because that would mean one tenant across two pools, which P2
forbids. That is the model's ceiling and it is a real one. The escape hatch, undesigned: bind at
index-pattern granularity rather than per tenant, so a single enormous tenant could span pools. It would
cost the single-writer property on `tenant/<t>/status/cluster`, which is the one document that spans a
tenant's indices.

### Where pools are defined

Three operator-written key kinds (`SUPERVISOR_KEYS.md`), none written by the control plane, which has no
concept of a pool and should not acquire one:

```
pools/definitions/<pool-id>    { "kind": "shared", "default": true, ... }
pools/bindings/<tenant-id>     { "pool": "shared-a", "generation": 4 }
pools/nodes/<node-id>          { "pool": "dedicated-acme" }        — optional override
```

**A node's pool comes from its own registration document** (`NODE_CONTRACT.md`), which it takes from its
deployment configuration — so a node deployed into a pool needs no separate write. `pools/nodes/<node-id>`
**overrides** it when present, which is how a node moves between pools without being redeployed.

**Tenant binding is one key per tenant, not a list inside the pool document.** That is the whole reason for
the layout: rebinding is then a single compare-and-set. A list-in-the-pool layout makes a move two writes
to two keys with no transaction between them, and the torn state is "this tenant is in both pools" — which
is two supervisors placing its shards, the exact failure P2 exists to prevent.

**Unbound is a finding, not a default.** A registered tenant with no binding raises `TENANT_UNPOOLED`; a
node whose pool does not exist raises `NODE_UNPOOLED`. A pool may declare `"default": true` (at most one) to
absorb unbound tenants, which is convenient for a long tail and dangerous for a large tenant that lands in a
shared pool unnoticed. Off unless configured.

## 3. Ownership, membership and balancing — with one lock

**The only mutual-exclusion primitive is one lease per pool**, `supervisor/pools/<pool-id>/lease`, acquired
and renewed by compare-and-set on the key's revision (R6.2). There is no global lock, no consensus, no
leader-of-leaders, and no membership protocol beyond records that expire.

```
supervisor/members/<supervisor-id>    who is alive, what they hold, how much they can take
supervisor/pools/<pool-id>/lease      the only lock
supervisor/leases/housekeeping        one more, for chores that belong to no pool
```

Each supervisor, each pass: renew its member record, renew the leases it holds, and consider one claim or
one release.

### The balancer is advisory; the lease is authoritative

This is the sentence that makes "minimal locking" true. The claim rule below is a heuristic run
independently by every supervisor from data that may be stale, and **they do not have to agree**. If two
supervisors both decide to claim the same pool, the compare-and-set serialises them: one wins, one gets a
`RevisionConflictException` and moves on. Disagreement costs a wasted CAS, not correctness.

So the balancer can be simple, approximate, and changed later without touching anything that matters.

- **Fair share** = total pool weight ÷ total member capacity weight, over live members.
- **Claim** an unowned or expired pool while my held weight is below my fair share. Jitter the attempt, so
  five supervisors starting together do not stampede one pool.
- **Release** my least-loaded pool when my held weight exceeds fair share by more than a hysteresis band
  (default 25%) for C consecutive passes (default 20). Both numbers exist to stop oscillation — the
  characteristic failure of every rebalancer, and the one R7.3 already watches for at the shard level.
- **Prefer what I hold.** Rebalancing throws away a warm revision cache and costs a full re-observation, so
  the bias is toward stability rather than toward a perfectly even split.
- **Pool weight** is published by the owner in the pool's convergence document (nodes + indices ÷ K).
  An unowned pool has no published weight and gets a configured default until someone claims it.

### Failover

A supervisor that dies stops renewing. Its leases expire and its pools are claimed by others; its member
record expires and its capacity leaves the fair-share denominator. **Failover time is the lease TTL**,
default `max(3 × pass interval, 15 s)`, and it is the number to argue about — too short and a GC pause
hands a pool to someone else, too long and a pool goes unmanaged.

`POOL_UNOWNED` is raised by the housekeeping holder for a pool no member has claimed, so a pool that nobody
can serve is visible rather than merely quiet.

### The stale writer, worked through

The failure that actually happens: supervisor A observes pool P, pauses for 30 s, loses P's lease to B, and
wakes holding a plan computed from state that is now old. Three defences, in order:

1. **`observation.isFresh()`** discards a pass whose observation is older than a bound (default 5 s). This
   catches it almost always and needs no coordination — and it fires for an ordinary GC pause too, not just
   for a lost lease.
2. **The lease is re-checked immediately before applying**, narrowing the window to the apply itself.
3. **Every write is a compare-and-set on the revision read during observation.** If B has written that key,
   A's write fails and is dropped; the next pass recomputes.

And where all three miss — A writes a key B has not touched, inside the window, with a fresh-looking
observation — **the write is harmless, because reconciliation is level-triggered.** B computes from the same
observed state and either agrees, in which case A did B's work, or disagrees and overwrites next pass.
Level-triggering is what makes a lost lease a performance problem instead of a correctness one, and it is
the strongest argument for R2.1 this design produces.

**It does not extend to non-idempotent side effects.** Nothing in scope today has any. Snapshot repository
operations (R5.3) will, and will need real fencing rather than this argument.

### Housekeeping

A few chores belong to no pool: detecting registered-but-unbound tenants, detecting nodes in no pool,
reporting unowned pools, and reaping member records long expired. They are given one small lease,
`supervisor/leases/housekeeping`, rather than a singleton service or being duplicated by everyone. One extra
lease, uncontended, and the work is seconds per pass.

## 4. Moving a tenant between pools

Two different things get called migration, and only one of them is solved.

### Ownership handover — solved, and it is nothing

Which supervisor manages a pool is a lease. Handover is the lease expiring or being released. There is no
state to transfer, because §1 keeps nothing across a pass that cannot be recomputed.

### Rebinding a tenant to a different pool — the hard one

The binding is one key, so the *decision* is atomic; the **data** is not. Two-phase, both phases on that
single key, so there is never an instant with two owners:

```json
{ "pool": "shared-a", "moving_to": "dedicated-acme", "generation": 4 }
```

1. **Operator CAS sets `moving_to`.** The owner of `shared-a` keeps ownership, stops placing new shards for
   that tenant, and quiesces. The owner of `dedicated-acme` does nothing yet — it can see the intent and
   must not act on it.
2. **The outgoing owner CASes to `{ "pool": "dedicated-acme", "generation": 5 }`** once quiesced, and drops
   the tenant. Handover is a single write performed by the party giving it up, which is the only ordering
   that never has two owners.

That moves the responsibility. Moving the **shards** is unsolved, and here is the honest state of it:

| option | verdict |
|---|---|
| **Move the nodes, not the data** — rebind the nodes holding the tenant's shards via `pools/nodes/<id>` | **the supported path.** Free when the tenant is alone on those nodes, which is exactly the case of a shared pool being carved up along node lines |
| **Loan** — let the incoming owner write assignments for nodes still in the outgoing owner's pool | **rejected.** It breaks P1, which is the invariant everything else rests on |
| **Drain across the boundary** — the outgoing owner writes only its own nodes, the incoming owner only its own, and the shard is copied by the data plane | plausible, undesigned; needs a cross-pool recovery source in `NODE_CONTRACT.md` |
| **Snapshot and restore** | needs no coordination at all, costs downtime or a dual-write window, and reuses machinery R5.3 requires anyway |

`FOREIGN_COPY` — a copy of a tenant's shard on a node outside its pool — is expected during a rebind and an
alarm at any other time. It is the finding that makes an abandoned half-migration visible.

**This is the least finished part of the design**, and it is worth knowing that before committing to the
pool model rather than after.

## 5. The loop

Per pool, independently:

```
while (running) {
    if (!lease.heldAndFresh(pool)) { balancer.considerClaim(); sleepUntilNextPass(); continue; }

    Observation observation = observe(pool);      // scans, bounded, one snapshot
    Diff diff = diff(observation);                // pure
    project(diff, observation);                   // R7 — happens even in read-only mode
    if (!readOnly) {
        Plan plan = plan(diff, policy);           // pure
        if (lease.heldAndFresh(pool) && observation.isFresh()) apply(plan);
    }
    sleepUntilNextPass();                         // or wake early on a watch
}
```

**`project` runs before `apply`, and runs in read-only mode.** The observability surface reports what was
seen, not what was done. It must be right before anything acts, and it is the entire deliverable of
milestone 3.

**Nothing survives a pass.** No plan, no partial state, no resume. R2.3 is satisfied by having nothing to
lose. The only cross-pass state is a cache (§7) and finding-age bookkeeping (§8), both safe to lose.

**A watch only shortens the sleep.** A notification carries no payload and may be missed, so the loop is
identical with and without one:

```java
Watches.watchOrPoll(store, "tenant/", Duration.ofSeconds(2), loop::wake)
```

The loop must be provably correct with the watch removed, and the test for that is to run the suite with
watching disabled and expect identical results, only slower.

## 6. Diff: what counts as wrong

One function per concern over the same observation, concatenated — each independently testable, and
independently disableable, which matters when one of them turns out to oscillate in production.

| finding | raised when | severity |
|---|---|---|
| `UNASSIGNED_PRIMARY` | an open index's shard has no primary anywhere | critical |
| `UNASSIGNED_REPLICA` | fewer copies than `index.number_of_replicas` | warning |
| `SURPLUS_COPY` | more copies than desired | warning |
| `ORPHANED_COPY` | a copy whose index uuid is absent from desired state | warning, **acts only after K passes** |
| `FOREIGN_COPY` | a copy on a node outside the tenant's pool | info during a rebind, critical otherwise |
| `STALE_CONFIG` | a copy's `config_revision` ≠ the index document's revision | warning |
| `NODE_STALE` / `NODE_LOST` | heartbeat revision unchanged past the thresholds | warning / critical |
| `NODE_UNREGISTERED` | heartbeat without registration | warning |
| `NODE_UNPOOLED` | a node in no pool, or naming a pool that does not exist | warning |
| `TENANT_UNPOOLED` | a registered tenant with no binding | critical — it is entirely unmanaged |
| `POOL_UNOWNED` | no live member holds the lease | critical |
| `TORN_DESIRED_STATE` | alias naming an absent index; data stream naming an absent backing index | **info until it ages** |
| `TASK_STUCK` | a task claimed past its lease, or at max attempts | warning |
| `PROJECTION_STALE` | a `status/` document older than N passes | warning |
| `UNPLACEABLE` | desired copies no live node in the pool can satisfy | critical |

Two of these are the ones to get right.

**`TORN_DESIRED_STATE` is expected.** R2.5 promises the supervisor *will* see an index without its alias.
Alarming the first time is how a correct system earns a reputation for crying wolf. So it is raised at
`info` and escalates on **age** — `passes ≥ 10`. That rule generalises: for every finding, the alarm is that
it is *old*, never that it exists.

**`ORPHANED_COPY` is where data gets destroyed.** A deleted index and an index document that has not landed
yet are indistinguishable from the store. Never act until the finding has held for K consecutive passes
(default 3) with an otherwise-consistent view of that tenant. Waiting costs disk on a deleted index; not
waiting costs the shard.

## 7. Bounded work, and the cache

R2.6: *"A pass over ten thousand indices must not be one giant write."*

**Observation** is bounded by not re-reading what has not changed. `scanPrefix` returns `Versioned`, and
revisions compare for equality on the same key, so the supervisor keeps `key → (revision, parsed)` and
re-parses only on a change. Shard inventories are not scanned at all in steady state — the heartbeat's
`shards_digest` says whether a re-read is needed (`NODE_CONTRACT.md`). The cache holds parsed values only;
losing it costs CPU on one pass, which R2.3 requires.

**Action** is capped per pass (default 200 instructions), divided across the pool's tenants with a per-tenant
floor so one large tenant cannot starve the others in a shared pool. Hitting the cap is normal for a
backlog and is published, because a permanently saturated supervisor looks exactly like a converged one
from outside.

**Projection** writes a `status/` document only when its content changes. Rewriting per-index health every
pass for 10,000 unchanged indices would make the supervisor the store's dominant writer, ahead of the nodes.

## 8. Convergence and oscillation

R2.4 wants convergence *detectable*, which needs cross-pass memory of a specific shape:

- **Finding age** — `(tenant, subject, kind) → firstObservedPass`, cleared when the finding clears. This is
  what makes "stable and non-empty" expressible, which `REQUIREMENTS.md` names as the alarm.
- **Action count** — `(tenant, subject) → ring of recent (pass, instruction)`. Oscillation is ≥ K actions
  (default 5) within a window (default 50 passes) without the finding clearing, or a subject alternating
  between two finding kinds.

Both in memory, bounded by finding count, both projected. Losing them on restart re-arms detection within
one window, which is fine for a diagnostic.

**On detecting oscillation the supervisor stops acting on that subject** and says so loudly. A subject
frozen with a visible reason beats a shard moving for ever.

## 9. Modules

```
supervisor-model/    records and JSON codecs. No dependencies. No store.
supervisor-core/     observe, diff, plan, apply, leases, balancer, loop. Depends on store-api and model.
supervisor-app/      main, configuration, wiring, logging.
fake-node/           a test double that registers, heartbeats, and obeys assignments.
```

`supervisor-model` having no store dependency is enforced by the module graph rather than by discipline: it
makes "diff is a pure function" checkable at build time instead of at review time.

`fake-node` is not a nicety. It is milestone 2's deliverable, the only way to exercise the loop before a
data plane exists, and — since it must implement `NODE_CONTRACT.md` from the document alone — the first real
review of whether that contract is implementable.

Java 21 toolchain and Gradle Kotlin DSL, matching the control plane, with the composite build per §8 of
`REQUIREMENTS.md`:

```kotlin
includeBuild("../opensearch-kv-store")
implementation("org.opensearch.controlplane:store-api:0.1.0-SNAPSHOT")
testImplementation(testFixtures("org.opensearch.controlplane:store-api:0.1.0-SNAPSHOT"))
```

Group and version must stay exactly as written or substitution misses and the coordinate resolves against a
repository instead of the local project.

## 10. Testing

Risk goes where no network is needed.

| layer | how | runs in the sandbox? |
|---|---|---|
| `diff`, `plan` | hand-built `Observation` literals, exhaustive | yes |
| codecs | round-trip, plus size-budget assertions | yes |
| loop, leases, apply | `InMemoryKeyValueStore` from the store-api test fixtures | yes |
| **balancer and failover** | N in-process supervisors over one in-memory store | yes |
| convergence, oscillation | scripted pass sequences | yes |
| real etcd / ZooKeeper | Testcontainers, else `-Dcontrolplane.<backend>=host:port`, else **skip** | no — skips |
| real OpenSearch | health compared against `_cluster/health` | no — skips |

**The balancer is testable here in full**, which is a happy accident of it needing only a store: several
supervisor instances in one JVM against one `InMemoryKeyValueStore`, with passes driven manually. Kill one
mid-pass, assert its pools are claimed within the TTL and that no two ever held the same lease. That is the
riskiest new machinery and it needs no Docker.

Two details to steal from the kv-store build rather than rediscover: the `@EnabledIf` condition method must
catch `Throwable` and answer false, because a throwing condition fails the class instead of disabling it;
and it must then print the reason to stderr, because a silent skip when Docker *is* running is worse than
the bug it fixed.

**Size budgets are unit tests.** Generate a node with 5,000 shard copies, serialise, assert under 512 KiB.
A budget nothing checks is a comment.

**The habit that matters most** is `WORKING_NOTES.md`'s: *"do not trust a passing test that you wrote
against your own model."* Unit tests and implementation share the author's assumptions, so a misread field
makes both wrong together and agreeing. Health computation gets compared against a real cluster's
`_cluster/health`; revision semantics get probed against real etcd through its HTTP/JSON gateway, which the
proxy carries and which is the one piece of real backend behaviour reachable from in here. Its JSON omits
false and zero fields, so a failed transaction has no `succeeded` key.

## 11. Milestones

1. **Skeleton.** Gradle + composite build, module graph, and the javac/JUnit fallback for this sandbox.
2. **The node contract.** Codecs for all four documents, size-budget tests, and `fake-node` writing
   registration, heartbeat and inventory. First real test of `NODE_CONTRACT.md`.
3. **Observe and diff, read-only, one pool.** Pool definitions, bindings, tenant discovery, the revision
   cache, liveness, `diff`, and the full R7 projection. **Acts on nothing.** Worth shipping alone: a service
   that answers "what is wrong" and cannot break anything.
4. **Horizontal scale, still read-only.** Member records, per-pool leases, the balancer, failover. Running
   several supervisors while none of them writes an assignment is the safest possible way to shake this out,
   and it is why this comes before acting rather than after.
5. **Act, narrowly.** One finding — `UNASSIGNED_PRIMARY` for an index with no copies anywhere — with the
   lease checks, the ceiling and the stale-writer defences in place. Everything else stays observation.
6. **The task lifecycle.** Claim, attempt, terminate, reap, so `cluster.pending_tasks` stops growing.
7. **Projections.** Health, then `cluster.state`, then awareness values. Each unblocks a 501.

Steps 3 and 5 are in that order deliberately, and `REQUIREMENTS.md` §10 argues it better than this file can:
a reconciler that observes correctly and acts on nothing is safe and immediately useful; one that acts
before it observes correctly is neither. Step 4 sits between them because leader election is easiest to
trust when the worst a bug can do is compute the same diff twice.

## 12. Open questions

1. **Cross-pool data migration** — §4. The supported path only covers the case where nodes can move with
   their data. Everything else is a sketch.
2. **The dedicated-pool ceiling** — a single tenant too large for one supervisor has no answer short of
   index-pattern binding, which costs the single-writer property on `tenant/<t>/status/cluster`.
3. **Tenant discovery** needs the control plane to write a `tenants/<id>` registry; proposal handed over at
   `../opensearch-control-plane/docs/proposed-tenant-registry.md`. Configuration is the fallback.
4. **Two contradictions in `REQUIREMENTS.md`**, both resolved in `SUPERVISOR_KEYS.md`, both worth
   confirming: R5.5 versus the control plane's tenant-scoped store, and R5.5 versus R4.3's requirement to
   reap tasks the control plane owns.
5. **Replica recovery source** — does the node find the primary through the status projection, or does the
   assignment name it? (`NODE_CONTRACT.md` open question 2.) Cross-pool recovery in §4 depends on the answer.
6. **Allocation policy is entirely undesigned.** This document says where the decision is made and how it is
   bounded, logged and reversible; it says nothing about how to choose a node. Awareness, disk watermarks
   and rebalancing are their own document.
7. **Rollover.** `generation` only advances on rollover, which the control plane does not implement. By the
   lifetime rule it is the supervisor's, but that is not yet agreed with the other side.
