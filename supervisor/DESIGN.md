# Supervisor design

How the service works, as distinct from what it must do (`REQUIREMENTS.md`) and what it reads and writes
(`KEY_LAYOUT.md`, [`NODE_CONTRACT.md`](NODE_CONTRACT.md), [`SUPERVISOR_KEYS.md`](SUPERVISOR_KEYS.md)).

Milestones 1 and 2 are built (§11); everything from §3 onward is still design to argue with rather than
code to read. Where implementing something changed the design, the section says so.

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

### A failure domain is what an operator says it is, not what an address implies

`refuse_copies_sharing_a_host` refuses two copies of one shard on nodes reporting the same address, and it is
**off by default**. It was unconditional, which was wrong on all three of the axes that matter:

- **It made us stricter than OpenSearch without saying so.** `cluster.routing.allocation.same_shard.host`
  defaults to `false` there — "no check is performed by default" — so an operator's expectations, formed on
  OpenSearch, were quietly violated by a placement engine that never explained the difference.
- **An address is wrong in both directions.** Too strict for several nodes deliberately run on one host, and
  far too weak in Kubernetes, where two pods on one physical machine have different addresses and share a
  failure domain completely. A rule that is wrong in the *dangerous* direction on the most common modern
  deployment is not a safety net; it is a comfort.
- **It broke the smallest useful deployment.** Every node of a single-machine deployment reports `127.0.0.1`,
  so no replica of anything could ever be placed. That is every developer's first two-node test, and it
  presented as a shard stuck unassigned while the diff insisted two nodes could take it — the exact shape of
  failure this design spends most of its effort trying not to produce.

`awareness_attributes` is the mechanism that expresses this properly, because only an operator knows which
node attribute names a thing that fails independently. This rule is a guess standing in for that, so it
defaults to not guessing, and an operator who knows an address really is a machine can say so.

**Found by the data plane, not by a test.** A search replica sat unplaced through a real three-plane run, and
the reason needed a code read rather than being visible in the diff — because `unplaceable_because` shows one
example refusal, and the example it chose was the *other* node's, refused correctly for already holding the
copy. Worth fixing separately: one refusal per distinct reason would have made this self-explaining.

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
| `PRIMARY_BEFORE_REPLICA` | a docrep replica was placed for a shard with no primary yet | warning |

Two of these are the ones to get right.

**`TORN_DESIRED_STATE` is expected.** R2.5 promises the supervisor *will* see an index without its alias.
Alarming the first time is how a correct system earns a reputation for crying wolf. So it is raised at
`info` and escalates on **age** — `passes ≥ 10`. That rule generalises: for every finding, the alarm is that
it is *old*, never that it exists.

**A move adds before it removes, always.** Relocating a copy is two writes to two nodes' assignments with
no transaction between them, so the order is the difference between a window with two copies and a window
with none. Add first, and do not remove the old copy until the new one reports `STARTED` — an extra copy is
a `SURPLUS_COPY` finding that clears itself, where a missing one is an outage. §12 has the audit.

**Document replication constrains the order of the plan, not just its contents.** A docrep replica resolves
its primary from `tenant/<t>/status/indices/<uuid>` and cannot make progress until one is serving, so a plan
that places replicas before primaries converges only by accident — and in the worst case not at all, if the
work ceiling keeps cutting the pass before the primary is reached. The planner must place a shard's primary
first and only then its replicas. Segment replication has no such constraint, which is why the rule is
per-index rather than global: it follows `index.replication.type`.

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

1. **Skeleton.** ✅ *Done.* Gradle + composite build, module graph, and `tools/jbuild.sh` — the javac/JUnit
   fallback for this sandbox. Carries the identifier types, `Keys` (the three contract documents as
   executable code), and `TenantDiscovery`, which is the thin vertical slice that proves the composite
   build, the store API and the test fixtures all resolve. `./gradlew build` verified green on Java 21.
2. **The node contract.** ✅ *Done.* Codecs for all four documents in `supervisor-model`, size budgets as
   tests, and a `fake-node` that registers, heartbeats, keeps an inventory and obeys its assignment.
   Implementing the contract from the document alone found five silences in it, all now decided and
   recorded there — the most consequential being that `generated_at` has to mean "last changed" rather than
   "last computed", and that an absent assignment is not an empty one.
3. **Observe and diff, read-only, one pool.** ✅ *Done.* Pool documents and resolution, tenant discovery,
   the revision cache, liveness from observed revision changes, the differ, and the R7 projection, tied
   together by `SupervisorLoop.pass()`. **Acts on nothing** — a test asserts it never writes an assignment.
   The end-to-end test drives a real `fake-node` rather than hand-written documents. Still single-pool and
   single-supervisor: leases and the balancer are milestone 4.
4. **Horizontal scale, still read-only.** ✅ *Done.* Member records, per-pool leases with compare-and-set,
   the balancer, housekeeping, and failover — driven by `Supervisor.tick()`. A fleet test runs three members
   over one in-memory store and asserts no pool is ever held twice, including through a rolling restart.
   Still writes no assignment, which a test enforces. Placement is milestone 5.
5. **Act, narrowly.** ✅ *Done.* One finding — `UNASSIGNED_PRIMARY`, and only where the shard is neither
   held nor already assigned anywhere in the pool — behind the age threshold, the work ceiling, the
   per-tenant floor, and all three stale-writer defences from §12. `Planner` only ever *adds* a copy, so the
   worst a wrong decision can do is create a surplus copy: a self-clearing finding rather than lost data.
   Node selection is deliberately the simplest defensible thing (most free disk, deterministic tiebreak) and
   is not the allocator this will need — see open question 7. Everything else in the diff stays observation,
   and a read-only mode is retained because it is the safest way to introduce this to a live cluster.
6. **The task lifecycle.** ✅ *Done.* Claim by compare-and-set, bounded attempts, complete, fail, reap — so
   `cluster.pending_tasks` starts telling the truth as soon as claiming works, since it reports only
   `pending`. Completion is decided from the observation rather than from having acted, which is what makes a
   duplicated task harmless and a lost one irrelevant; tests assert reconciliation with the queue completely
   empty (R2.1) and with a task duplicated (R4.4). Failed records are never reaped, because a deleted failure
   is an invisible one.
7. **Projections.** ✅ *Done.* Per-index health and placement, the cluster summary, the tenant-scoped node
   list, and the discovered awareness values — computed purely from an observation and written only when they
   change. Snapshot metadata (R5.3) is the one piece still outstanding, and is deferred because it needs
   repository access, which is the only part of this service that talks to something other than the store.

A read-only supervisor projects none of these, which is worth knowing before relying on read-only mode as a
way to observe a live cluster. It follows from a decision taken in milestone 2: one document serves four
readers — `cluster.health`, `cluster.state`, coordinator routing and docrep peer resolution — which was right
for write amplification, and means health cannot be published without publishing routing that coordinators
will act on. Splitting the routing half out would allow observation without behaviour change; not worth doing
until somebody wants it.

Steps 3 and 5 are in that order deliberately, and `REQUIREMENTS.md` §10 argues it better than this file can:
a reconciler that observes correctly and acts on nothing is safe and immediately useful; one that acts
before it observes correctly is neither. Step 4 sits between them because leader election is easiest to
trust when the worst a bug can do is compute the same diff twice.

## 12. Torn reads, and where they can cost more than a pass
### 12.1 Measured, not assumed

The reasoning below was written before any of it had been observed. It has now been checked against both
backends by `AbstractStoreAssumptionsIT`, with a writer sweeping keys in ascending order while a reader scans
the prefix 200 times:

| backend | snapshot-consistent | torn | widest generation spread |
|---|---|---|---|
| etcd | 200 | 0 | 2 |
| ZooKeeper | 11 | 189 | 3 |

Two things worth taking from that. **etcd's scans are atomic and ZooKeeper's are not**, which confirms the
premise this section rests on rather than leaving it as an inference from the two implementations. And a
generation spread of **2 is normal for an atomic scan**, not a symptom: the SPI has no multi-key transaction, so
a sweep of *n* keys is *n* writes and a correct snapshot taken mid-sweep necessarily straddles two of them. A
spread of **3** is what a walk looks like, because one instant cannot hold three generations.

The practical consequence is that torn reads are not a hypothetical this design tolerates in principle — on
ZooKeeper they are what almost every scan returns. Everything below is load-bearing there.



The store has no cross-key transaction, and **`scanPrefix` is not a snapshot either**. Its contract is
"every key beginning with this prefix, in ascending key order" and nothing more: etcd serves it as one
range request and is therefore atomic at a revision, but ZooKeeper walks the subtree with separate
`getChildren` and `getData` calls, and the filesystem store walks a directory. So an `Observation` is a
collection of reads taken at different instants, and every composite this system builds can be torn.

This section is the audit, because "level-triggered, so it converges" is true of almost all of it and
dangerously close to true of the rest. The distinction that matters is not whether a read can be torn —
they all can — but **whether acting on the torn view costs a pass or costs data.**

### The rule that makes most of it safe

Every action is either idempotent or gated. A torn read then produces a wrong *decision*, the next pass
observes reality again and produces a different one, and the cost is latency. That covers:

| torn read | what the reader may see | cost |
|---|---|---|
| assignment vs. the index document it names | config revisions disagree | node applies what it read; supervisor re-issues |
| assignment vs. `status/indices` | projection predates the assignment | peer resolution returns "not yet"; retry |
| `status/indices` vs. `status/nodes` | a peer's address read after its placement | stale address; connection fails; retry |
| heartbeat vs. inventory | digests disagree | documented already: use the inventory, re-read next pass |
| two nodes' inventories | a shard seen on both, or neither, mid-move | `SURPLUS_COPY` or `UNASSIGNED_*`; both self-clear |
| pool binding vs. node pool | a supervisor acts on a node just moved to another pool | CAS on the observed revision loses the race |

### Three that were not safe, and are now

**A node resolves its index document by name, and its assignment identifies the index by uuid.** Delete an
index and recreate it with the same name and the node can read the successor's settings and mappings while
building a shard for the predecessor's uuid — silently, because both documents are individually valid. The
node must compare the `uuid` field in the index document with the uuid its assignment named and treat a
mismatch as "not present yet". `NODE_CONTRACT.md` now requires it.

**A heartbeat and an inventory can straddle a restart.** A node writes registration, then inventory, then
heartbeat, so within one process the inventory is never older than the heartbeat that announces it — but
across a restart the supervisor can read the previous process's heartbeat with the new process's inventory.
Both carry `ephemeral_id`, so it is detectable; the contract now says to check, to prefer the inventory, and
to treat the ack as void when they disagree.

**A move is two writes to two nodes' assignments.** Remove-then-add leaves a window with no copy at all; the
planner must always add before removing, and must not count the new copy until the node reports it serving.
This was implied by "a move is a copy added and then a copy removed"; it is now a stated planning rule
rather than an inference.

### The one that a retry cannot fix

**Promoting a primary is two writes and there is no fencing token.**

Moving a primary from node A to node B means writing A's assignment and B's assignment, in some order, with
no transaction. Every ordering has a window:

- **Promote B first:** A and B both believe they are primary. Under document replication both accept
  writes, and the two copies diverge. No later pass repairs that — the data is already different.
- **Demote A first:** the shard has no primary and rejects writes until B is promoted. Availability, not
  correctness, and recoverable.

Ordering alone cannot close it, because the failure that matters is not the write window. A partitioned or
paused node A never sees its demotion at all: it holds an assignment saying `PRIMARY` and, by this
contract's own level-triggered rules, is behaving correctly by continuing to serve as one. The supervisor
cannot revoke something by writing a key the other party is not reading.

**The hazard follows the ingestion path, not the replication mechanism.** That correction matters enough to
state plainly, because the obvious framing — "docrep is unsafe, segrep is fine" — is wrong in both
directions:

| ingestion | two primaries means |
|---|---|
| **pull-based** (primaries read an ordered external source) | duplicated work, but no divergence *of the data*: both consume the same source at the same offsets and produce equivalent documents. |
| **push-based** (clients write to the primary) | divergence. Two primaries accept different writes and neither copy is authoritative — regardless of whether documents or segments are replicated afterwards. |

So the fencing requirement is partly a property of how data arrives, and pull-based ingestion removes the
*divergence* half of it.

**It does not remove the other half, and this is the part that is easy to miss.** Under segment replication
against a remote store, two primaries for the same shard write segments to **the same remote path**. The
documents they produce may be identical and it does not help: two writers on one path is corruption, at the
storage layer, below anything the ingestion source can make consistent. Pull-based ingestion makes
concurrent primaries *safe to serve from* and does not make them *safe to store from*.

Which means neither replication mode is exempt:

| mode | two primaries costs |
|---|---|
| docrep, push | divergent data — no later pass repairs it |
| segrep, push | divergent data **and** a corrupted remote path |
| segrep, pull | a corrupted remote path |

**The intended direction is segment replication with pull-based ingestion**, which removes the divergence
problem by construction — a much stronger position than fencing writers apart — and leaves the storage one.
Push-based document replication is expected anyway, for adoption, and there the resolution is decided
rather than open:

- **Demote before promoting** — accept a window with no primary. §12 called that "availability, not
  correctness, and recoverable", and it is the branch to take: a shard that rejects writes for a moment is
  a far better failure than two copies that disagree for ever.
- **Increment a primary term on each promotion**, so a partitioned old primary — which never sees its
  demotion and is behaving correctly by this design's own rules — has its writes rejected rather than
  accepted. This is OpenSearch's mechanism, and `cluster-etcd`'s per-copy `allocationId` plays part of the
  same role.

### What the remote path needs is a generation, not a term

A primary term fences *writers against each other*. The remote store problem wants something slightly
different and more useful: **a unique identifier for a generation of a primary**, with each generation
writing to its own path and able to bootstrap from a previous one.

That turns two live primaries from a corruption into an ordinary state, and it buys a capability the term
alone does not:

- **Split brain becomes survivable.** A primary that stops heartbeating gets replaced; the new one writes a
  new path rather than the old one, so the partitioned original corrupts nothing when it comes back.
- **Upgrade with rollback, without downtime.** Bring up a new primary on the new version, restore from the
  old generation's path, write to a new one, and migrate search replicas across gradually. Keep the old
  primary alive and rollback is free, because its path was never touched.

That second one is why this is worth more than a safety fix: two concurrent primaries stop being a hazard
to tolerate and become a deployment mechanism. It also explains the gradual search-replica migration — the
replicas are being moved between generations, not merely re-pointed.

**Carried as a known gap.** Nothing in this design has either a term or a generation. Part of it is not
even ours: writing to a per-generation path and bootstrapping from a previous one is a property of
OpenSearch's remote-store segment replication, so the supervisor side — assigning the generation, naming
the one to restore from — cannot be built ahead of it. The supervisor's eventual share is small: a
generation identifier per shard in the assignment and in `status/indices`, and a rule that a new primary is
given a fresh one.

**A primary handover is not instantaneous under segment replication either.** Search replicas replicate
with respect to a particular primary, so promoting a new one means moving them across gradually rather than
switching them in one write. That makes a search replica still following the old primary an expected
transient during handover rather than a fault, which the diff has to know — otherwise it reports a fleet of
findings every time a primary moves. It is also a second reason the planner cannot treat promotion as a
single step.

### Why the projection is safe to resolve peers from

One invariant carries more weight than it looks: **`tenant/<t>/status/indices/<uuid>` is derived from
observation, never from intent.** It names a node as primary only after that node reported itself primary.
So the projection lags reality and never leads it, and a replica reading it can point at a node that *was*
primary but never at one that has merely been *asked* to become primary.

That is what makes peer resolution safe against a torn read in one direction. In the other direction — a
primary that has been demoted since the projection was written — it is the same hazard as above, and it
wants the same fix.

## 13. Open questions

1. **No fencing identity for a primary — carried as a known gap**, and partly not ours to close. §12:
   promoting a primary is two writes with no token, so two nodes can hold the role at once. Pull-based
   ingestion removes the data divergence but not the remote-store path collision, so no replication mode is
   exempt. The shape of the answer is a per-shard **primary generation** rather than only a term, since
   per-generation paths also buy upgrade-with-rollback; it depends on OpenSearch's remote-store segrep
   supporting a new primary writing a new path and bootstrapping from the previous one.
2. **Cross-pool data migration** — §4. The supported path only covers the case where nodes can move with
   their data. Everything else is a sketch.
3. **The dedicated-pool ceiling** — a single tenant too large for one supervisor has no answer short of
   index-pattern binding, which costs the single-writer property on `tenant/<t>/status/cluster`.
4. **Tenant discovery** needs the control plane to write a `tenants/<id>` registry; proposal handed over at
   `../opensearch-control-plane/docs/proposed-tenant-registry.md`. Configuration is the fallback.
5. **Two contradictions in `REQUIREMENTS.md`**, both resolved in `SUPERVISOR_KEYS.md`, both worth
   confirming: R5.5 versus the control plane's tenant-scoped store, and R5.5 versus R4.3's requirement to
   reap tasks the control plane owns.
6. ~~**Primary before replica.**~~ Closed: it is `ReplicaAfterPrimaryDecider`, which is the better shape for
   it — the rule is a property of a candidate placement rather than a phase of the planner. The planner also
   orders primaries first, so a pass converges in one round instead of relying on the decider's wait.
7. ~~**Rebalancing, which the allocator still does not do.**~~ Closed; see §14. What remains is that a
   rebalance can only move a **replica**, so a pool whose imbalance is all in primaries cannot be relieved —
   which is open question 1 wearing a different hat, and is asserted as a failing-when-fixed test rather than
   only described.

   Deciders deliberately not implemented, with the reason: `NodeVersionAllocationDecider` (ordering during a
   rolling upgrade), and everything about snapshot and restore state, which this service does not model.
   `EnableAllocationDecider` is now implemented — with the caveat that `new_primaries` is applied as `none`,
   because telling a never-allocated shard from one whose copies have all gone needs per-shard history nothing
   here keeps.
8. **Functional rather than complete task completion.** `SUPERVISOR_KEYS.md` §2: at cloud scale you never
   have 100% of what you asked for, so "complete convergence" is a definition that eventually stops being
   satisfied. The replacement asks whether the thing is *usable* — and is genuinely per task type, "is the
   index reachable" for a create against "is it unreachable" for a delete, which is why it is not the
   starting point. Revisit when complete convergence stops being reachable at the scale being run.
9. **Rollover.** `generation` only advances on rollover, which the control plane does not implement. By the
   lifetime rule it is the supervisor's, but that is not yet agreed with the other side.

10. **An ordinary replica of a segment-replication index cannot be asked for.** `IndexConfig.desiredCopiesOf`
    maps `number_of_replicas` onto `SEARCH_REPLICA` when the index uses segment replication, so that setting and
    `number_of_search_replicas` are *added together* and both produce search replicas. OpenSearch treats them as
    independent — ordinary replicas peer-recover and can be promoted; search replicas never can — and this cannot
    express that.

    It is kept because it is **a contract with the data plane, not a detail of this service**. The plugin
    dispatches on the assigned role rather than on the replication type: it builds a `SEARCH_REPLICA` when a
    remote store backs the index, and currently *defers* a `REPLICA` with "a document-replication replica needs
    peer resolution, which this node does not implement yet". So changing the mapping unilaterally would turn
    working search replicas into deferred ones. Closing it means the two sides agreeing at once, and it is worth
    doing when the plugin implements peer resolution — until then the change would buy an inexpressible
    configuration for a broken one.

---

## 14. Balancing

Placement decides where a *new* copy goes; balancing decides whether an *existing* one is in the wrong place.
They share the deciders that define a legal placement and share nothing else, because they answer different
questions and the second one is the one that can make a cluster worse.

### 14.1 Why a rule interface, and why OpenSearch's balancer is hard to improve

The allocation side of OpenSearch is a set of `AllocationDecider` classes: one rule each, individually
testable, each able to explain its own refusal. The balancing side is not. The decision to move a shard is
spread across `AllocationDecider#canRebalance`, the weight function inside `BalancedShardsAllocator`, and a
threshold — so adding a consideration means working out which of those three it belongs in, and usually
editing a weight function that every other consideration also shares. The rules cannot be listed, cannot be
tested one at a time, and cannot be explained individually to the operator asking why their cluster is
lopsided.

The consequence that matters in practice: **OpenSearch balances on shard count**. A node holding a hundred
one-megabyte shards and a node holding one hundred-gigabyte shard look identically loaded, so a cluster can be
perfectly balanced by the only measure the balancer has and still have one machine holding most of the data.

So there are two interfaces here rather than one, split along the line that was blurred:

- **`RebalanceDecider`** — a veto. `RebalanceEnabledDecider`, `RecoveryPriorityDecider`, `PrimaryMoveDecider`,
  `MoveThresholdDecider`, `ConcurrentRebalanceDecider`. Same three verdicts as allocation, and `THROTTLE`
  earns its keep: almost every reason to refuse a move is a reason to refuse it *now*.
- **`BalanceSignal`** — a measure. `ShardCountSignal`, `ShardSizeSignal`. A rule that can only say no cannot
  express "this shard is four times the problem that one is", which is the thing a balancer needs to know.

A move's *destination* is judged by the ordinary `AllocationDecider` set, reused rather than restated. A
balancer with a private notion of a legal placement is one that will eventually put a copy somewhere
allocation would have refused, and the two will disagree quietly.

### 14.2 Making signals comparable

Shard count is measured in shards and shard size in bytes, so they cannot be added: a naive weighted sum is
dominated entirely by whichever signal uses the larger unit, and the weights an operator sets would do almost
nothing. Each signal is divided by its own mean across the pool before being combined, so every signal
contributes about its `importance` to an average node.

A consequence worth knowing: **when every shard is the same size, size and count are the same signal**, and
their relative weight cannot matter. The weights only bite once sizes are skewed, which is the case they exist
for. Defaults are size 1.0 and count 0.5 — size leads because bytes are the cost an operator feels, and count
survives because it proxies the per-shard costs that do not scale with size (heap for segment metadata, file
handles, a slice of every refresh and merge), the same ground `ShardsLimitDecider` guards as a hard ceiling.

Two things the load model must get right beyond arithmetic. A copy that has been **assigned but not yet
reported** counts against its target from the moment the assignment is written, or a node handed ten replicas
one second ago still looks empty and gets ten more. And because such a copy has no size yet, and a shard of
unknown size weighs nothing, the size is filled in from **the largest copy of the same shard elsewhere in the
pool** — not a guess, an observation of that shard, since a replica ends up about the size of the primary it
was built from.

### 14.2.1 Heat, as a signal

Heat is the obvious next signal, and the interface was shaped to take it: a new `BalanceSignal` class, not a
new branch in an existing one. `NODE_CONTRACT.md` covers the document; what matters on this side is four
things, three of which are constraints on how the signal may be *used* rather than on how it is measured.

**The node smooths, not the supervisor.** Smoothing here would mean per-shard history in supervisor memory,
which breaks R2.1 and evaporates on lease handoff. The node publishes an already-smoothed figure and declares
its own half-life. The supervisor should refuse a window too short to act on — balancing on a ten-second
average is worse than not balancing at all, because a move takes minutes and would land after the spike had
passed.

**The window is the hysteresis.** With a ten-minute half-life a thirty-second spike moves the average a few
percent and cannot clear `MoveThresholdDecider`'s margin. No separate spike detection is needed; the noise
filter and the patience are one mechanism. This is the whole reason the smoothing has to be somewhere durable.

**Unknown is not zero.** A node that publishes no heat looks cold, and a cold node attracts work — so heat is
usable only when *every* placeable node in the pool publishes it, and its importance drops to zero for the pool
otherwise. Partial heat data is actively worse than none. This mirrors `size_bytes` being omitted rather than
zero, but the failure mode is sharper: an unsized copy merely fails to attract work, whereas an unheated node
*attracts* it.

**Heat is not conserved the way bytes are, and this is the one piece of arithmetic that changes.** Moving a
copy transfers its bytes and transfers its traffic, so the load model's `A - w` / `B + w` holds for both. But
changing a shard's *copy count* splits traffic rather than duplicating it: going from `n` to `n+1` copies drops
each existing copy to roughly `n/(n+1)` of its former heat, and the new one takes about `1/(n+1)`. That is
perfectly computable — it is simply a different formula, and `MoveThresholdDecider`'s inequality is specific to
moves. Scaling *down* has the mirror hazard and it is the dangerous direction: removing a copy concentrates its
traffic onto the survivors, so "this copy looks cold, drop it" can tip the remainder over. The test there is
whether the survivors can absorb `heat / (n-1)`, which is the same shape as the move threshold and needs to be
written as its own rule. See §15.

### 14.3 Cost is per node, because it is not symmetric

`MoveCost` reports two figures rather than one, and this is the part a scalar cost model gets wrong.

Under **segment replication to a remote store**, the source does nothing: the segments are already in the
object store and the target restores from there. The bill is the target's download, and a node being drained
can be drained as fast as the targets can pull.

Under **document replication** — and node-to-node segment replication, which is the same shape and which as
far as anyone knows nobody runs — the segments come off the source's disk and out of its network interface.
The move costs both ends, and moving twenty shards off one overloaded node at once makes it worse at the job
it is already behind on.

`ConcurrentRebalanceDecider` therefore meters the target always and the source **only when the source pays**.
A single-scalar cost has to pick a side: meter the source always and it refuses to drain a remote-store node
at the speed it could be drained; meter it never and it hammers a document-replication node that is already
the bottleneck. Neither is fixable with a setting, because the right answer differs per index.

### 14.4 Why it terminates

Moving a copy of normalised weight `w` from a node at load `A` to one at load `B` leaves them at `A - w` and
`B + w`. Spread is the gap between the busiest and idlest node, so the move reduces it only if the target does
not end up worse than the source began:

```
B + w < A     i.e.     A - B > w
```

At `A - B == w` the move swaps the two nodes' positions and achieves nothing. Below it the move *overshoots*,
the pool is more lopsided than before, and the same rule applied to the new state proposes moving it straight
back — the exact shape of an oscillating balancer, where every individual move looks locally sensible and the
cluster moves data forever. Requiring strict improvement makes the loop terminate, because spread is
non-negative and every accepted move strictly decreases it. The threshold adds a margin on top (10% of the
copy's weight) for hysteresis, since loads come from an observation that is always slightly out of date.

**One move per pass**, deliberately. Proposing several means predicting the pool's load after each accepted
move, and a prediction is what a level-triggered reconciler exists to avoid: two moves that each improve the
same starting state can jointly overshoot. Re-observing costs one pass.

### 14.5 A move is an addition, and then, separately, a removal

`Instruction.RemoveCopy` is the only destructive instruction, and what makes it safe is that **it is never
paired with an addition**. A move is not "add there, remove here". It is an addition now, and then — one or
more passes later, from observed state alone — an independent decision that some shard has more *started*
copies than it wants and that this is the one to drop. Nothing records that a move was in progress.

A supervisor that held "I am moving shard 3 from A to B" in memory would have to decide what to do when it
crashed between the halves, when its lease moved mid-move, or when the target never built the copy — and every
one of those is a way to drop the source copy with nothing to replace it. Deriving the removal from observed
state means there is no in-between to recover: at every instant either the pool has a surplus copy, which is
untidy and safe, or it does not.

Three properties follow, rather than three precautions:

- Redundancy never dips, not even momentarily, because a removal is only proposed when the copies that would
  remain are **already started**.
- A crashed supervisor leaves a surplus copy, which the next lease holder clears by the same rule.
- A failed move costs bandwidth, not availability: if the target never builds its copy, the surplus never
  appears and the original is never dropped.

And the move still lands where it was aimed — not by luck, but because `MoveThresholdDecider` only accepts a
move when the source exceeds the target by more than the copy weighs, so after the target gains `w` the source
is *still* the heavier of the two, and "drop the copy on the busiest node holding it" therefore picks the
source. The rule that stops oscillation is the rule that makes the move complete in the intended direction.

`SurplusReaper` deliberately does **not** reap a copy whose index is absent from desired state. That is
`ORPHANED_COPY`, which the differ already ages over several passes because an absent index may be a torn read
of the control plane's keys rather than a deletion; reaping it as a surplus would bypass that patience.

### 14.6 Priority, and the feedback loop it breaks

Three kinds of shard work compete for the same recovery bandwidth and are worth wildly different amounts:
missing copies, then surplus copies, then rebalancing — and rebalancing only when a pass has found nothing
else to do at all.

`RecoveryPriorityDecider` counts **started** copies, not assigned ones, and that distinction is the rule. A
lost node leaves its shards short *and* the pool lopsided; the planner places replacements; those copies are
now initializing. A balancer that counted them as redundancy would start shuffling shards between the
survivors at exactly the moment they are busiest rebuilding what was lost — slowing the rebuild and extending
the window in which a second failure loses data. Counting only started copies keeps the rule honest in the one
scenario it was written for. The cost, stated plainly: a pool with enough churn that something is always
recovering will rarely rebalance. That is the right trade, and the operator is told which rule is holding it
up.

### 14.7 What an operator can read

`cluster.routing.rebalance.enable` is separate from `cluster.routing.allocation.enable`, and the separation
matters: turning off tidying during a rolling restart is normal and healthy, and must not also stop a lost
shard being rebuilt. The implication runs one way — no allocation means no rebalancing, since a move begins
with an allocation; no rebalancing does not mean no allocation.

The pool's load spread and one sentence about it go in the **pool convergence document**, not a tenant diff.
A pool whose diff is legitimately empty and whose nodes are visibly uneven is otherwise a question with no
answer written down anywhere. "No single move would reduce it" is a real answer, and it deliberately
suppresses every other refusal when it holds: an evenly loaded pool full of primaries would otherwise be
reported as blocked on the primary-move gap, sending an operator to chase a limitation costing them nothing.

---

## 15. Replica count as a target, not a constraint

**Status: not implemented, and deliberately not foreclosed.** This section exists to record a goal and to name
the one place in the code where the distinction has teeth, so that the eventual change is a small one.

OpenSearch assumes every shard of an index has the same replica count, because `index.number_of_replicas` is a
single number and the routing table is built from it. Nothing here inherits that assumption — the supervisor
computes placement per shard from observed state, and the number it compares against happens to come from an
index-wide setting only because that is where the control plane keeps it. The eventual goal is **automatic
scaling of replica counts per shard, up and down**: a hot shard gains a copy while capacity exists, a cold one
gives one back.

### 15.1 Configured versus authorised

The change that makes this possible is semantic rather than structural. Today one number answers two different
questions:

- **How many copies did the operator ask for?** — the floor, and the thing health is judged against.
- **How many copies should the pool be maintaining right now?** — the *authorised* count, which an autoscaler
  may raise above the configured floor.

Once those are separate, "more copies than configured" stops meaning "surplus". It means either *surplus* (a
move completing, a returned node, a reduced setting) or *authorised extra*, and only the first should be
reaped.

### 15.2 The seam, and the one place that would otherwise fight it

`IndexConfig.copiesAuthorisedFor(int shard)` is the authorised count. It returns
`max(copiesPerShard(), copyOverrides[shard])` — so the configured count is a floor and an override below it is
ignored, because an autoscaler able to reduce redundancy below what an operator asked for is a different and
much less welcome feature. Nothing populates the overrides yet; `withCopyOverrides` exists so that tests can,
and so the mechanism is exercised rather than merely described.

Of the five sites that read a desired copy count, three now go through the seam and two do not:

| site | reads | behaviour with authorised extra copies |
|---|---|---|
| `SurplusReaper` | authorised | leaves them alone — **the site that mattered** |
| `Planner` | authorised | builds them by the ordinary placement path |
| `AllocationContext.desiredCopiesOfThisShard` | authorised | consistent with the above |
| `Differ` | configured | would report `SURPLUS_COPY` — noise in the diff |
| `StatusComputation` | configured | derived counts read high — cosmetic |

`SurplusReaper` is why this was worth doing now rather than later. It deletes copies above the count, one per
pass, so an autoscaler adding a copy would have watched the reaper remove it on the next pass, forever, with
the two burning recovery bandwidth arguing. That is a genuine foreclosure rather than a missing feature, and it
is closed.

`Planner` reading the authorised count is the other half: a reaper that declines to delete an extra copy is
worth little if nothing will create one. Routing placement through the same seam means an autoscaler raises a
number and the existing machinery builds the copy — it does not need to become a second writer of assignments,
which would break the single-writer property the whole pool partition model rests on.

The two remaining sites are deliberately left. Both are read-only projections, both would merely be *wrong on
paper* rather than destructive, and changing `Differ`'s signature is a wider refactor than this is worth before
anything actually populates an override. They are the known cost of stopping here.

### 15.3 What an autoscaler would still need

Recorded so the shape is clear, not as a plan:

- **A durable target.** Same argument as heat smoothing (§14.2.1): a scaling decision held in supervisor memory
  is lost on lease handoff, so the authorised count has to be written down. Most likely a supervisor-owned
  document, which also makes it answerable — "why does shard 3 have four copies" should have a written answer.
- **Conservative fallback.** If that document is missing or unreadable, the reaper must decline to reap above
  the configured count rather than assume the extras are unauthorised. Leaving extra copies is safe and visible;
  deleting them is not, and an unreadable document is exactly when the supervisor knows least.
- **Its own decision rules,** because the move-threshold inequality does not transfer. Scaling up splits
  traffic; scaling down concentrates it, and the second is the direction that can cause an outage. §14.2.1 has
  the arithmetic.
- **Asymmetric patience.** Scale up quickly and down slowly, since the cost of being wrong is a wasted node in
  one direction and a hot shard with too few copies in the other.
- **A floor that is never crossed.** Whatever the autoscaler decides, the configured count remains a floor and
  health is still judged against it. An autoscaler that can reduce redundancy below what an operator asked for
  is a different and much less welcome feature.

---

## 16. Explaining a refusal

A placement that cannot happen produces one verdict per candidate, and showing an arbitrary one of them wasted
the point of having rules that explain themselves — worse, the arbitrary choice was biased towards the useless
answer. The node legitimately holding the primary sorts early and is always refused, so "that node already has a
copy" reliably hid whatever was actually blocking progress.

So `Allocator` shows **one example per distinct reason**, most-affected first:

```
no node was allowed by every decider; 5 candidate(s) refused for 2 distinct reason(s):
  node [n1]: [same_shard] node [n1] shares address 127.0.0.1 with node [n0], which already has
    a copy, and this pool refuses copies that share a host (4 nodes);
  node [n0]: [same_shard] node [n0] already holds, is assigned, or is about to be given a copy
    of this shard; a second copy on one node adds no redundancy at all (1 node)
```

Three decisions in that, each of which the obvious implementation gets wrong.

**Grouping is on a reason code, not on the prose.** `Decision` carries a short slug — `shared_host`,
`high_watermark` — beside the sentence, and the key is the set of `(decider, code)` pairs that refused. Grouping
on the text cannot work, because most reasons name the node or a measurement and so differ for every candidate:
three nodes over the disk watermark produce three different sentences about the same rule. Grouping on the
decider *name* is too coarse in the other direction, and that is not hypothetical — the regression that prompted
this had one decider (`same_shard`) giving two genuinely different answers, so a per-decider example would still
have shown only one of them. Only the decider knows what varies and what does not, so it says.

The code is **required** for a refusal and defaulted for an approval. Nothing groups approvals, and making it
mandatory where it matters means the compiler finds every new rule that can block a placement.

**Ordering is by number of nodes affected.** This is what makes the output systematically useful rather than
merely complete: the reason blocking four of five candidates is far likelier to be the actionable one than the
reason blocking one, and it is also the one that survives the cap. Node id breaks ties, so two supervisors reading
one observation write one sentence.

**Two bounds, both dropping whole clauses.** At most three distinct reasons, and a character budget on top,
because each clause names *every* rule that refused that candidate and so is not short. Characters are never cut
inside a sentence — a reason truncated mid-way is worse than a reason replaced by a count — and the number
omitted is always stated. In practice the reason cap binds first, which is the right way round.

**The throttled-only case is left exactly as it was.** "Every candidate is throttled, so this is paced rather
than blocked" is already the whole answer, and grouping identical pacing messages would bury it in arithmetic.

---

## 17. A search-only copy needs a store to read from

Two placement rules, both found by the real data plane, and both reading fields that already existed.

### 17.1 The rule

`RemoteStoreDecider` refuses a `SEARCH_REPLICA` on a node whose attributes name no
`remote_store.segment.repository`. OpenSearch chooses a replica's segment source from the **node's** attributes,
and the branch it takes for a node without a remote store resolves the primary through the local routing table —
which a clusterless data node's fabricated cluster state does not contain. So such a copy starts, reports itself
`STARTED`, and never receives a segment. It looks perfectly healthy and serves nothing.

The plugin already declines this with a reason, so a placement was never *unsafe* — it converged, because the copy
was reported absent and the diff kept asking. What it wasted was a round trip per pass, for ever, on a placement
the supervisor had everything it needed to avoid.

**Keyed on the segment repository attribute, not on "is this a remote-store node".** This mirrors the data plane
exactly, and the reason is specific: OpenSearch's `isRemoteStoreNode()` also requires a *cluster-state*
repository, which a node running `remote_store.mode=segments_only` does not have — and that mode exists for this
architecture. Gating on the broader predicate would refuse the very nodes this design expects. **The two sides
have to ask the same question, or one places what the other declines.**

Only search replicas are judged. A primary's requirements are different and more involved — it may need a translog
repository to recover at all — and the plugin declines that case itself rather than starting a shard empty over
its own data. Encoding half of that rule here would be worse than leaving it whole over there.

### 17.2 The check only this service can make

`POOL_REPOSITORY_MISMATCH` is `CRITICAL`, and it is the only finding whose failure mode is **a lie rather than an
absence**:

```
nodes in this pool write segments to 2 different remote repositories: [segments-east] on 1 node,
e.g. [nodeAAA…]; [segments-west] on 1 node, e.g. [nodeBBB…]. Copies of one shard share a remote
path, so a search-only copy placed on the wrong node reads an empty store and reports itself
healthy. No node can detect this -- each sees only its own attributes -- so nothing else will
report it.
```

That last clause is the justification for the check existing at all. Every other party in the system sees one
node's configuration. The supervisor sees all of them at once, which makes it the only thing that can notice they
disagree — and if it does not, nothing does, because the symptom is a healthy-looking empty shard.

Nodes declaring **no** repository are not a disagreement and are not counted. A coordinator has none, and a data
node without one is refused a search-only copy by §17.1 with its own reason. Absence is somebody else's finding.

### 17.3 What this cost, which is the interesting part

Adding the decider broke fifteen tests, and every one was a fixture whose nodes declared no repository while the
test asserted about search replicas being placed. They were modelling **a pool that could not actually serve the
copies they were asserting about** — the decider did not break them, it revealed them. Remote store is now the
default in every node fixture, and tests about this rule override it.

Worth stating because it generalises: a rule that refuses something nothing had refused before will fail every
test that assumed the old permissiveness, and the useful question is always whether those tests were describing a
configuration that could have worked.

---

## 18. Coordinators are membership, not placement

Every other assignment this service writes is the end of a chain that starts with a finding: the diff says a copy
is missing, the planner places it, the applier writes it. **A coordinator entry has no such chain**, and that is
why nothing wrote one for so long — `TenantAssignment.coordinator()` existed and its only caller was a test.

There is no shard to place, so there is nothing to find missing, and inventing a finding per (node, tenant) would
fill the diff with items that are never *wrong*: a coordinator without a tenant is not a deficiency, it is a node
waiting to be told what it is for. So it is computed where the assignment document is assembled, as a property of
the assignment rather than as an instruction: *for each node in this pool with the coordinator role, for each
tenant bound to this pool, carry `coordinator`.* Both inputs are already in the observation.

Three consequences, each of which had to be handled rather than noticed later.

**It does not consume the work ceiling.** Nothing is moving, and a pass that spent its budget placing shards
should still be able to tell a new coordinator what it serves. Making it an instruction would have put it in
competition with shard recovery for no reason.

**The applier no longer skips an empty plan.** It used to return immediately when there was nothing to place —
which meant a pool with no shard work never told its coordinators anything, and a coordinator's whole job is
precisely the case where nothing is being placed.

**Which makes a no-op guard load-bearing.** Every placeable node is now considered every pass, so writing an
identical assignment would bump its version and revision for ever; the node reads that as a change and liveness
reads it as activity. Two cases, and the second is easy to miss: an assignment whose tenants map is unchanged is
not rewritten, and **a node that was never told anything and has nothing to be told is left alone** rather than
handed an empty document, because an absent key already means exactly that. Missing the second wrote an empty
assignment to every idle node.

**Removal matters more than it looks.** A tenant rebound to another pool must lose its coordinator entry, or a
coordinator keeps routing queries to shards a different supervisor now owns — the hazard `DESIGN.md` §4 discusses
for rebinding, arriving through a document nothing used to write. It falls out of computing the desired set from
the currently bound tenants rather than tracking changes: a tenant no longer bound is simply not in the set.

### 18.1 Why `roles` became a set

`TenantAssignment.roles` is a `Set<NodeRole>` rather than one value, because **a node holding copies of an index
and coordinating for its tenant is a real configuration** — and the natural one for a small deployment, since
OpenSearch runs a coordinator on every node out of the box. That is a poor idea once there is enough hardware to
separate work by type, and exactly the right idea for three nodes on a laptop.

The alternative was to let `DATA` imply the capability and rule that a node whose *registration* declares the
coordinator role coordinates for everything it has an assignment for. No format change, but it makes a node's
behaviour depend partly on its own configuration rather than wholly on the instruction written for it, which cuts
against the grain of everything else here. **A node should be able to learn everything it does from its own
document.** Registration `roles` say what a node may be given; the assignment says what it *is* given; keeping
those separate is what lets a coordinator be moved between tenants without touching the node.

The supervisor writes only the plural form and still reads the singular one, because assignments outlive the
process that wrote them — a supervisor that has just been upgraded reads its predecessor's documents, and
reporting every node's assignment as unreadable for one pass during an upgrade is when nobody wants novel
findings. The tolerance costs one pass and then nothing.

---

## 19. Two ways a shard stops being placed, and neither said so

Both found from a real pool that would not place a shard on its one live data node, after enough data planes had
been started and stopped to leave a graveyard of them.

### 19.1 The differ and the planner disagreed about what a copy is

`Differ.actualCopies` counts a copy only when a node **reports** one — it requires a non-null inventory.
`AllocationContext.nodesWithThisShard` also counted a copy a node had merely been **assigned**. Those are
different questions and the difference is load-bearing:

- an *inventory* is the node's own statement about what it has;
- an *assignment* is the **supervisor's** statement about what it should have, and is only evidence of a copy if
  the node is in a position to be acting on it.

A node handed a primary that died before reporting it therefore satisfied the planner and not the differ. The
result was the worst shape a bug can take here: the shard was announced `UNASSIGNED_PRIMARY` every pass, and the
planner skipped it **before the allocator ran**, so there was no placement and not even a refusal to read. A
reproduction is in `PlannerTests`; it produces `instructions=[] refusals={}`, which is exactly what the artifacts
from the real pool showed — nothing.

`NodeObservation.actsOnItsAssignment()` is now the single answer, and it requires a **heartbeat** rather than
`LIVE`. That distinction matters in the other direction: a node that has heartbeated and gone quiet keeps its
claim through the stale window, which is the GC-pause tolerance the whole liveness ladder exists to provide —
placing a second primary while the first may still be running is the one mistake here that costs more than a pass.
What does not survive is a node that never spoke at all.

### 19.2 A refusal was filed against the shard, so it was reported against the wrong finding

Three kinds of unassigned copy share one subject — the shard — and `Plan.refusals` was keyed by subject. So on a
one-node pool where a shard wants a primary and a search replica, the primary is placed, the search replica is
then correctly refused because the only node has just been given the primary, and **the refusal is rendered
against the primary's finding**:

```
UNASSIGNED_PRIMARY :: index [x] shard 0 has no primary on any node in this pool
  unplaceable: [same_shard] node [N] already holds, is assigned, or is about to be given a copy
```

Whoever reads that goes looking for a node that "already holds" an index it was never assigned, and the stored
documents do not support the claim — because the claim is about a different copy. That is precisely the
observation the data plane's write-up could not explain and attributed to "a third thing happening that these
artifacts do not show". There was no third thing; the evidence was mislabelled.

`Finding.refusalKey()` includes the kind, and `Plan.refusalFor(finding)` is the only way to read one, so nothing
has to know how the key is built. When the key was the subject, both sides agreed with each other and were wrong
together — which is the failure mode a shared helper exists to prevent.

**The general lesson is the same in both halves: two pieces of code answering one question separately will
eventually answer it differently, and the symptom appears somewhere neither of them is looking.**

---

## 20. Nothing reaps a node that is gone

Reported from a real pool, by a coordinator that declined to route to one of two nodes claiming one socket:

```
91HTBQFsTkKh3teM0PZlQw: [127.0.0.1:9300] is already claimed by node [ENQgP1TRSr6FelzOfPWL6g],
so this one is not routed to -- usually a projection for a node that was replaced on the same
port and has not been reaped yet
```

The diagnosis in that message is right. **Nothing in this service deletes a departed node's documents.**
`Housekeeping` reaps *member* records — other supervisors — and `StatusProjector` reaps projections whose key it
did not write this pass, which does not help: a dead node's `nodes/registration/<id>` is still there, so it is
still observed, so its projection is still written.

And a fresh supervisor process makes it worse before it makes it better. Liveness is revision silence measured from
when **this** process started watching, so every restart of the supervisor grants every corpse in the pool a full
staleness window during which it is `UNKNOWN` rather than `LOST` — and `UNKNOWN` is published, because
`copiesCount()` only excludes `LOST`. Restart a data plane on the same port a few times and the store accumulates
one registration per incarnation, all of them claiming the same address and port.

### 20.1 What is fixed: one node per endpoint

A process owns a socket, so two nodes on one `address:port` means one of them is history. The projection now
publishes only the freshest — liveness first, then the more recent heartbeat, then node id for determinism — and a
node with no heartbeat at all ranks last, never having demonstrated it is there.

**Only the supervisor can make this call**, which is what makes it belong here: each node sees its own registration
and no other, so it cannot know it has a predecessor, while the supervisor sees every registration at once. Same
shape as the repository-mismatch check in §17.2.

Keyed on `address:port` and not on address alone, deliberately: several nodes on one machine with their own ports
is the ordinary shape of a laptop deployment, and collapsing them would break the case this whole feature exists
to serve. A test covers it, because the first version of the *other* test proved nothing — a registration with no
assignment is not projected at all, so the collision could not occur until the fixture also left behind the
assignment a replaced node really does leave.

### 20.2 What is not fixed: the reaping itself

This treats the symptom. The cause is that a departed node's four documents live for ever, and the honest reason
it is not fixed here is that **three of them are not the supervisor's to delete**. `nodes/registration/<id>`,
`nodes/heartbeat/<id>` and `nodes/shards/<id>` are written by the node; the whole key layout rests on every key
having one writer, and reaping means deleting keys whose writer is presumed gone rather than known to be.

The hazard is specific and not hypothetical. A node partitioned from the store for longer than the grace period is
indistinguishable from a node that has died — and if its registration is deleted while it is merely unreachable,
it comes back, resumes heartbeating, and is now a node with a heartbeat and no registration: `NODE_UNREGISTERED`,
placeable for nothing, needing a restart to recover. Trading a slow leak for a node that strands itself is not an
improvement.

Two ways out, and both are more than a patch:

- **Give the node contract a rule that a node rewrites its registration when it observes it missing.** Then reaping
  is safe by construction and self-healing, at the cost of one read per pass on the node side. This is the better
  answer and it is a change to `NODE_CONTRACT.md`, which means agreeing it with the data plane.
- **Reap with a compare-and-set on the revision observed**, and only after the `NODE_LOST` finding has aged the way
  the planner ages everything it acts on. That closes the window to the width of one pass rather than eliminating
  it, and it needs the observer to carry registration and heartbeat revisions, which it currently does not.

Until then the leak is bounded by operator patience rather than by code, and the symptom that actually hurt — a
reader handed two nodes on one socket — is gone.

---

## 21. Pausing ingestion, and the one thing here that is not desired state

Pull-based ingestion is configured through index settings, so it needed no contract at all. Two things about it
are not settings, and they are as different from each other as anything in this system.

### 21.1 The pause is ordinary, once you know where it lives

Whether a stream is consuming is a first-class field on OpenSearch's `IndexMetadata` — `IngestionStatus.isPaused`
— and **not** a setting. The obvious guess, `index.ingestion.paused` in the settings block, finds nothing. The
control plane writes it at the top level of the index document, matching upstream's spelling:

```json
{ "index": "logs", "uuid": "...", "state": "open",
  "ingestion_status": { "is_paused": true },
  "settings": { "index.ingestion_source.type": "kafka", ... } }
```

From there it is a mapping change wearing a different hat: the data plane fabricates metadata from that document
every pass, so the change reaches a node level-triggered, and reversing it is equally ordinary — nobody should add
a latch. `pause-ingestion` and `resume-ingestion` complete on the same condition as `update-settings`: **every copy
is running the current configuration revision**, which the inventory already reports per index. No new field
anywhere.

Both types previously parsed as `UNKNOWN`, which never completes, so every pause turned into a permanent
`TASK_STUCK` finding. That is now fixed, and it is the whole of the easy half.

### 21.2 A consumer-pointer reset is an instruction, and instructions do not belong in desired state

A resume may carry `reset_settings`: per shard, move the consumer to this offset or timestamp. Upstream applies
these as a transport action to the shards and persists nothing, which is exactly right and exactly unavailable
here — there is no cluster manager to send it.

**It cannot go in the index document**, and the reason is the design's own principle turned against it. Desired
state is a value a reconciler re-applies for ever without harm. "Start from offset 1000" re-applied for ever is a
stream that never advances: every pass would rewind it. So the control plane puts it on the **task**, which is this
system's existing channel for *do this once, then reap it*.

That leaves the supervisor with the hard part, and it is worth naming precisely. **You cannot detect that a reset
has been applied by looking at the consumer's position.** A shard reset to offset 1000 immediately starts
advancing, so by the time anything looks it is at 1200 — indistinguishable from a shard that was never reset and
happened to be at 1200 already. Worse for the interesting case: rewinding a consumer that is at 5000 back to 1000
looks, before it is applied, exactly like a job already done, so "is the pointer past the target" would report
success and drop the instruction *before* anything acted on it.

**So do not detect the effect. Require an acknowledgement.** That is the same move the contract already makes for
assignments: a node does not report what its assignment contains, it reports which *version* of it the node
applied. Proposed:

- The reset goes into the assignment of the node holding that shard's primary, carrying the **task id** as its
  identity:

  ```json
  "indices": { "<uuid>": { "name": "logs", "config_revision": 3041, "shards": { "0": "PRIMARY" },
      "ingestion_reset": { "id": "<task-id>",
                           "shards": { "0": { "mode": "offset", "value": "1000" } } } } }
  ```

- The node applies it and reports the ordinary `applied: { assignment_version, outcome }`. When the version it
  reports equals the version now in the store and the outcome is `applied`, the reset in that store document has
  been acted on — **no memory on the supervisor's side, and no new field on the node's.**
- The supervisor then completes the task and stops writing the reset, which is a property of assembling the
  assignment rather than an instruction, exactly like coordinator membership in §18.

### 21.3 The sequencing that does not work, and why it is not a race

The first version of §21.2 said the node "applies it" and left the ordering implicit. It does not work as written,
and the reason is worth keeping because it is a general hazard rather than a detail of ingestion.

Upstream **refuses a reset on a poller that is running**:

```java
// IngestionEngine.resetStreamPoller
if (streamPoller.isPaused() == false) {
    throw new IllegalStateException("Cannot reset consumer when poller is not paused");
}
```

And the control plane writes `is_paused: false` and *then* submits the task carrying the resets. So the two halves
of one operator action reach the node down paths of very different lengths:

- `is_paused: false` is **desired state**, in a document the node already reads every pass. It converges on the
  next one, unconditionally.
- the reset is an **instruction**: observed by the supervisor, planned into an assignment, written, read. Several
  passes at best.

**The fast path is the wrong one, so the ordinary case is that the poller resumes and the reset then throws.** This
is not a window to be narrowed — it is the normal ordering, and narrowing it would only make the failure rarer and
harder to attribute. Any design that mixes desired state with instructions about the same object inherits this
shape: the state arrives first because it has less to travel through.

The obligation therefore sits with the node, and the contract has to say so rather than leave it to be discovered:
**a node applying an `ingestion_reset` pauses the poller first, applies the reset, and only then lands on whatever
pause state the index document actually asks for.** Two calls, because upstream's own
`updateIngestionSettings` applies the reset *before* its pause flag, so one call carrying both still runs the reset
against a live poller. That sequence is correct for a poller that was already paused, one resumed a pass ago, and
one never paused at all — and it costs a brief stop on a running stream, which is inherent to resetting a consumer
rather than something this design adds.

A node that trusted the ordering instead would pass every test in which the reset happened to arrive first.

### 21.4 Why nothing needs to remember having applied one

`resetStreamPoller` ends with `flush(true, true)` — a force flush to persist the new batch start pointer — so a
reset lands in the Lucene commit, and a data plane that prefers local bytes on recovery reads its start pointer
back out of the commit's user data. **A shard that moves after a reset therefore resumes from the reset point with
the instruction existing nowhere at all.**

That is what makes the whole scheme safe rather than merely convenient. The supervisor can stop writing the reset
the instant it is acknowledged, because the reset's effect is durable in the shard rather than in the document that
asked for it. Nothing has to remember, on either side, which is the property that lets a one-shot instruction live
in a level-triggered system without a ledger.

### 21.5 Completion when a reset spans nodes

A reset is per **shard**; an assignment is per **node**. So a task naming shards 0 and 1 whose primaries sit on
different nodes needs *both* acknowledgements, and the task is complete only when every named shard's holder
reports `applied` at the version now in its own assignment — plus the ordinary metadata condition, since a resume
is also an unpause.

A shard whose primary is unassigned cannot acknowledge at all. That task stays **incomplete with a reason**, not
completed and not failed: the same patience every other kind of work here gets, and for the same reason — the
shard's absence is a placement problem that the diff is already reporting, and blaming the resume for it would
hide both.

### 21.6 One spelling for the mode

Three repositories could each guess differently, so it is pinned here. Upstream's enum is
`StreamPoller.ResetState.RESET_BY_OFFSET` and `RESET_BY_TIMESTAMP`. The control plane validates the wire value
case-insensitively and stores **verbatim** what the client sent, so `"OFFSET"`, `"offset"` and `"Offset"` can all
arrive in a task.

The supervisor therefore **parses tolerantly and writes one canonical spelling**: lower case, `offset` or
`timestamp`, in the assignment. A mode it does not recognise reads as `UNKNOWN` rather than failing the task
record, on the same reasoning as every other wire enum here. `value` is always a string — an offset, or epoch
milliseconds for a timestamp — and the node maps it to the upstream enum.

Until both sides agree §21.2's obligations and §21.3's sequencing, `resume-ingestion` carrying resets deliberately
**does not complete**. It says why, at length, in the task's own reason. Completing it would be the worst available
outcome: a report that a pointer had moved when nothing moved it, in the one case where nothing downstream can ever
contradict it.
