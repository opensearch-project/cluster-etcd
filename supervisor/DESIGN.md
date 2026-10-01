# Supervisor design

How the service works, as distinct from what it must do (`REQUIREMENTS.md`) and what it reads and writes
(`KEY_LAYOUT.md`, [`NODE_CONTRACT.md`](NODE_CONTRACT.md), [`SUPERVISOR_KEYS.md`](SUPERVISOR_KEYS.md)).

Milestones 1–11 are built (§11), and §§1–23 describe code rather than intentions. §24 is future work and says
so; §25 is a principle rather than a component. Where implementing something changed the design, the section says
so — and there is rather a lot of that, because most sections were written before the code and several were wrong
in ways only the code found.

## 0. Decisions taken, and what they cost

| decision | alternative rejected | what it costs |
|---|---|---|
| **Independent of `cluster-etcd`** (this repository's `main` branch) | port its controller onto `store-api` | rebuilding allocation, health and election; its prior art informs, its schema does not |
| **The node pool is the unit of ownership** | the tenant, or the whole cluster | a pool must fit inside one supervisor, and a tenant cannot span pools |
| **Horizontally scalable, one lease per pool** | a single leader | a balancer, member records, and a failover window |
| **Multi-tenant data model from day one** | single `default` tenant | every structure is tenant-keyed; tenant discovery needs a change to the control plane |
| **Observability projected into the store** | an HTTP endpoint | every observability document must be bounded, so the diff is truncated |
| **Read-only first** | act as soon as the diff works | a milestone that ships nothing user-visible, and is worth it |
| **Tenants and operators are different parties** (§25) | vanilla's one admin role | every `cluster.*` setting has to be classified by owner, and one namespace now has two |

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

*A pool is also the **operator's** unit, which §25 argues is a distinct role from the tenant's. Read that first if
the question is who may write a given setting.*


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

**A pool whose pass throws is given up the same way.** Leases are renewed at the start of a tick, before any pass,
so a member whose pass over one pool always throws would otherwise renew that pool for ever and no peer could take
it. `Supervisor.tick()` drops the pool without releasing it, runs the other pools and housekeeping regardless, and
rethrows. The lease expires after the TTL, which is also the pause before anyone, this member included, tries again.

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
| `ORPHANED_COPY` | a copy whose index uuid is absent from desired state, **held or merely assigned** | warning, **acts only after K passes** |
| `FOREIGN_COPY` | a copy on a node outside the tenant's pool | info during a rebind, critical otherwise |
| `NODE_STALE` / `NODE_LOST` | heartbeat revision unchanged past the thresholds | warning / critical; `LOST` is reaped once it ages (§20.2) |
| `NODE_UNREGISTERED` | heartbeat without registration | warning |
| `NODE_REGISTRATION_REWRITTEN` | a node recently put its own registration back — most likely this service reaped a live one (§20.3) | warning, **on the age of the last rewrite, never the count** |
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
yet are indistinguishable from the store. Never act until the finding has held, and §22 is the whole of what
"until" means — the patience depends on what kind of evidence there is, and the rule has several cases where
it must never act at all. Waiting costs disk on a deleted index; not waiting costs the shard.

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
includeBuild("../kv-store")
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
5. **Act, narrowly.** ✅ *Done.* Originally one finding — `UNASSIGNED_PRIMARY` — behind the age threshold, the
   work ceiling, the per-tenant floor, and all three stale-writer defences from §12, with node selection
   deliberately the simplest defensible thing. **That description is long out of date and is kept because the
   ordering argument below depends on it**: the point of the milestone was that `Planner` only ever *added* a
   copy, so the worst a wrong decision could do was create a surplus — a self-clearing finding rather than lost
   data. Four later milestones each gave up a piece of that safety on purpose, and each says what it bought.
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

Milestones 8 onward were not planned in advance. Each one came from running the thing — most of them from the data
plane finding something (`FINDINGS_FROM_THE_DATA_PLANE.md`), which is why they are grouped by what they gave up
rather than by feature.

8. **The allocator, and rebalancing.** ✅ *Done.* Nine deciders returning `YES`/`THROTTLE`/`NO` with a reason
   code each (§16), so a refusal explains itself and refusals group exactly rather than approximately. Then a
   separate `RebalanceDecider`/`BalanceSignal` split (§14): load normalised per signal because units cannot be
   added, move cost accounted asymmetrically because segment replication charges only the target, and a strict
   improvement threshold that is simultaneously the termination proof and the reason a move lands where it was
   aimed. A move completes as a *surplus* decision taken later by code that does not know a move happened.
   This is where the milestone-5 property went: the service now removes copies.

9. **Letting go, and the three things that needed it.** ✅ *Done.* `ORPHANED_COPY` acted on for the first time
   (§22), which turned out to be three bugs rather than unreclaimed disk — a deletion task that could never
   finish, and a node that could never report an assignment as applied, which silently disabled consumer-pointer
   resets. Then `Displacer` (§23), giving up a search-only copy so a shard can have a primary again, which is the
   first time this service deletes a copy that is currently *serving*. Then `NodeReaper` (§20.2), deleting a
   departed node's four documents, which needed a contract change agreed with the data plane to be safe at all.
   Each of these gave up a piece of milestone 5's guarantee, and each asks the allocator whether the removal
   achieves anything before making it.

10. **What a tenant can ask for, and what the operator answers.** ✅ *Done.* `number_of_search_replicas` honoured
    and copies counted per role (§17), coordinators assigned as membership rather than placement (§18), ingestion
    paused and resumed with a consumer-pointer reset handed over, acknowledged and withdrawn (§21) — the messiest
    thing here, because a pointer is the one instruction that is not desired state. And `DESIRED_COPY_IGNORED`, so
    a request this service declines leaves a reason behind instead of evaporating.

11. **Noticing that any of this has stopped.** ✅ *Done.* `MONITORING.md` stages 1 and 2: a rule library that is
    a pure function of a sample, `supervisor-app --alerts` with exit status as its interface, and the two
    documents the rules needed — `status/members/<id>`, which finally discharges R3.3, and
    `status/pools/<p>/reaping`, which makes the most destructive action reviewable. The part worth having is the
    rule a pass cannot evaluate about itself: a supervisor whose pass number has stopped moving looks exactly
    like one with nothing to do, from inside.

### What is not built

Short, and each item says whose it is:

- **Primary generation and fencing** — open question 1, and the largest gap by some distance. Promoting a primary
  is two writes with no token, so two nodes can hold the role at once. Partly not ours: it depends on OpenSearch's
  remote-store segment replication supporting a new primary writing a new path and bootstrapping from the previous
  one.
- **Snapshot metadata** — R5.3, and the only piece of this service that would talk to something other than the
  store. Deferred for exactly that reason rather than for effort.
- **The oscillation detector** — R7.3, and the one monitoring rule still blocked. It needs cross-pass memory of
  *actions* rather than of findings, and a window definition; the rest of §5.2 evaluates today.
- ~~**`STALE_CONFIG`**~~ **removed rather than implemented.** A copy whose `config_revision` is behind is not a
  problem, it is a copy mid-update: the condition is already the completion test for `UPDATE_MAPPING` and
  `UPDATE_SETTINGS`, so a stuck update shows up as `TASK_STUCK` — a finding that names the task, the attempts and
  the reason, which is strictly more useful than one naming the copy. A finding for the same condition would have
  fired on every configuration change in flight.
- **`Differ` and `StatusComputation` count *configured* copies, not authorised ones**, where `SurplusReaper` and
  `AllocationContext` count authorised (§15). So a shard legitimately scaled past its setting would raise a
  `SURPLUS_COPY` the reaper then correctly declines to act on — a permanent finding, which now also means a
  permanently non-empty diff and therefore a `DIFF_STABLE_NON_EMPTY` alert. The seam exists; two of its four users
  have not been moved onto it.
- **Two open questions belong to other repositories**: 12's rejection of an operator-owned setting written by a
  tenant is the control plane's, and 10's inexpressible ordinary replica needs both sides at once.

A read-only supervisor projects none of the milestone-7 documents, which is worth knowing before relying on
read-only mode as a way to observe a live cluster. It follows from a decision taken in milestone 2: one document serves four
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
   ingestion removes the data divergence, and the remote store's term ordering removes the path collision once
   terms advance — so under pull ingestion two primaries become a deployment tool rather than a hazard. **§27 supersedes most of §26**: the remote segment store already orders writers by primary term, so the
   path prefix is unnecessary and the generation is the primary term (finding 11). What remains is exclusion for push
   ingestion (§26.2) and, for rollback, a search-replica pin, forward-only in-place moves, and suspending cleanup —
   §27.3. The earlier short version, kept for its reasoning, is that the path prefix buys isolation and not
   exclusion, that the exclusion half is a node self-demotion rule needing nothing from upstream, and that the
   per-shard-versus-index-setting difficulty has a three-step ladder out of it whose first step asks nobody for
   anything.
2. **Cross-pool data migration** — §4. The supported path only covers the case where nodes can move with
   their data. Everything else is a sketch.
3. **The dedicated-pool ceiling** — a single tenant too large for one supervisor has no answer short of
   index-pattern binding, which costs the single-writer property on `tenant/<t>/status/cluster`.
4. **Tenant discovery** needs the control plane to write a `tenants/<id>` registry; proposal handed over at
   `../control-plane/docs/proposed-tenant-registry.md`. Configuration is the fallback.
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

11. **Announced deployments**, written up as §24 because the interesting part is which decisions it constrains
    rather than what it would cost. A deployment signal from the control plane carves the "back in a few minutes"
    case out of the middle of the liveness ladder, which is what would let the thresholds at either end stop being
    a compromise across three unrelated durations. Not implemented; §24.2 is the part worth reading before
    anything here is designed around it.

12. **A tenant's awareness attributes are read by nothing.** `ClusterSettings` parses exactly
    `cluster.routing.allocation.enable` and `cluster.routing.rebalance.enable`; awareness reaches
    `AwarenessDecider` from `PoolDefinition.awarenessAttributes` alone. So a tenant setting
    `cluster.routing.allocation.awareness.attributes` in its own cluster settings changes nothing about where its
    copies go — the same silent inertness as `index.number_of_search_replicas`, which is what `DESIRED_COPY_IGNORED`
    exists for.

    Found by the control plane while acknowledging the `status/awareness` narrowing
    (`../control-plane/docs/proposed-awareness-attribute-narrowing.md`): it validates weighted routing
    against the tenant's setting, so a weight can be accepted for an axis nothing spreads along.

    **Answered: no.** `zone` is a fact about a rack in a building — a statement about the machines, which a tenant
    cannot know and did not pay for — so it is the operator's to declare and the pool definition is the only place
    it can come from. §25 is the general rule this is an instance of, and it says what to do about it: an
    operator-owned setting a tenant writes is **rejected at the control plane's boundary**, not reported afterwards
    as a finding, because the tenant should learn from the request rather than from a traffic split that never
    happens. What remains here is the rejection, which is the control plane's to implement, and §25.5's prediction
    that this is the first of these rather than the only one.

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
`ORPHANED_COPY`, whose patience is §22 and is calibrated quite differently from anything here; reaping it as a
surplus would bypass all of it.

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

## 20. Reaping a node that is gone

Reported from a real pool, by a coordinator that declined to route to one of two nodes claiming one socket:

```
91HTBQFsTkKh3teM0PZlQw: [127.0.0.1:9300] is already claimed by node [ENQgP1TRSr6FelzOfPWL6g],
so this one is not routed to -- usually a projection for a node that was replaced on the same
port and has not been reaped yet
```

The diagnosis in that message was right, and at the time **nothing in this service deleted a departed node's
documents.** `Housekeeping` reaps *member* records — other supervisors — and `StatusProjector` reaps projections
whose key it did not write this pass, which did not help: a dead node's `nodes/registration/<id>` was still there,
so it was still observed, so its projection was still written.

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

### 20.2 The reaping itself

A departed node's four documents lived for ever, and the honest reason it went unfixed was that **three of them
are not the supervisor's to delete**. `nodes/registration/<id>`, `nodes/heartbeat/<id>` and `nodes/shards/<id>`
are written by the node; the whole key layout rests on every key having one writer, and reaping means deleting
keys whose writer is *presumed* gone rather than known to be.

The hazard was specific and not hypothetical. A node partitioned from the store for longer than the grace period
is indistinguishable from a node that has died — and if its registration is deleted while it is merely
unreachable, it comes back, resumes heartbeating, and is now a node with a heartbeat and no registration:
`NODE_UNREGISTERED`, placeable for nothing, needing a restart. Trading a slow leak for a node that strands itself
is not an improvement.

**What unblocked it was the contract, not this repository.** `NODE_CONTRACT.md` now requires a node to rewrite its
registration when it observes the document missing, with the same `ephemeral_id` and the same `registered_at`,
because neither the process nor the registration is new. A wrong reap therefore costs one pass and heals itself.
The data plane implements it and counts the rewrites, which matters more than it sounds: self-healing hides the
thing it heals, so one rewrite is an incident and a steady trickle is something reaping live nodes.

`NodeReaper` deletes all four keys, and four things decide whether it may.

**An aged `NODE_LOST`, not a fresh one.** A node that does *not* implement the rewrite rule is still conformant
with everything else in the contract, so this cannot be built on the assumption that every node self-heals. The
wait — thirty passes on top of the twenty heartbeat intervals of silence that raised the finding — is the
protection for the node that does not heal. It is also sized for a restart or a deployment, which is the wrong
thing for it to be sized for; §24 is what would let it stop being a compromise across three unrelated durations.
It is the same figure as `OrphanReaper`'s, for the same reason: both delete on the strength of an inference, and
the wait is for a human to notice that something is systematically wrong before it becomes irreversible.

**The verdict is re-read from the observation**, never trusted from the diff. A finding's age can outlive its
truth.

**Nothing the pool still needs.** This is what makes reaping destructive rather than tidy: an absent assignment
means *"hold nothing"*, so reaping a node whose copy the pool never managed to replace would turn the operator
action of bringing a machine back into the deletion of the last copy of a shard. So a node is kept — with a
reason — while any copy it holds **or was assigned** is one no other node reports as `STARTED`. Both records are
read, because a node assigned a copy that died before reporting it has no inventory entry for it. A copy of an
index that is gone or closed does not count: nobody wants it, and holding a node's documents for ever on account
of a copy of something that does not exist would make the graveyard permanent.

**A compare-and-set on the heartbeat, as the gate for everything after it.** The witness is the revision the
*observation* saw, which only `LivenessTracker` remembers — it is the same number the liveness verdict was
computed from, so deleting under it means "nothing has written here since I concluded this node was gone".
Re-reading the key first would prove only that nothing happened in the intervening microsecond. A node that wrote
anything in between fails the exchange and keeps all four documents.

The order is heartbeat, then inventory, then assignment, then **registration last**. A crash part-way through
leaves a registration with no heartbeat, which the observer already handles as "registered and has never spoken":
a grace period, then `LOST` again, then another attempt. The reverse order would leave a heartbeat with no
registration, which is the exact state this whole design is trying not to create.

### 20.3 Noticing that a reap was wrong

Reaping is safe because a node puts its registration back. That same rule is what would otherwise make a *wrong*
reap invisible — the node recovers, and nothing anywhere records that this service destroyed the documents of a
machine that was alive. **Self-healing hides the thing it heals.**

So the node counts the rewrites and reports them in its heartbeat (`NODE_CONTRACT.md`), and the supervisor raises
`NODE_REGISTRATION_REWRITTEN` while the last one is recent. Two things about the shape:

- **Keyed on the age of the last rewrite, never on the count.** The count never returns to zero within a process's
  lifetime, so a finding on "has ever happened" would stand for ever — and a finding that cannot clear is one an
  operator learns to scroll past. This is §6's law applied to a counter: the alarm is on age, and *has ever
  happened* is not an age.
- **It is this service reporting on itself**, which is the right place for it. The likeliest cause of a rewrite is
  §20.2 reaping a live node, and a reconciler that quietly destroys and recreates state is the failure this whole
  design is arranged against. Leaving it to an external monitor would put the supervisor's own worst mistake in the
  one document the supervisor does not write.

**The supervisor could detect some of this alone, and it is not a substitute.** It knows which nodes it reaped, so
a node reappearing with the same `ephemeral_id` after a reap was a live node wrongly reaped — no contract change
needed, and it attributes the mistake to the supervisor that made it. It catches strictly less, though: the memory
is lost when that supervisor crashes or hands over the pool, which is a plausible pairing since a reap and a crash
can come from the same bad state; and it only sees registrations deleted **by a reap**, where the node's counter
sees one deleted by anything — a stray script, a botched migration, a key-prefix change in another repository.
Those are the cases nobody thought to look for, which is what makes the counter worth asking for.

### 20.4 One thing neither side detects

Absence only. If something *overwrites* a registration with different content rather than deleting it, the node
does not notice — its decision to rewrite compares against its own in-memory copy, and its store read only asks
whether the key exists. The layout's one-writer rule says that cannot happen, so this is a gap in detection rather
than in behaviour. Worth knowing it is a gap, because "the node self-heals its registration" reads as stronger
than it is.

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

- The node applies it and reports the ordinary `applied: { assignment_version, outcome }`. The supervisor asks
  whether the node applied **a document that contained this instruction**: the assignment it was observed holding
  carries a reset with this task's id, and the node reports having applied *that* version, with outcome `applied`.
  **No memory on the supervisor's side, and no new field on the node's.**

  Not "is the node up to date", which is what the first implementation compared — and it completed the task before
  the reset had been written. The task queue runs after the applier but reads the observation taken *before* it, so
  on the pass a reset is first written the store holds version N+1 while the observation still shows N, and a node
  reporting N looked, by version alone, perfectly acknowledged. Asking the precise question removes any dependence
  on the store and the observation agreeing, and it makes the id earn its keep on this side too: an assignment
  rewritten for an unrelated reason keeps the same reset id, so a node that skipped re-applying it — as the contract
  requires — still acknowledges correctly at the newer version.
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

**With one bound, which is worth stating precisely because "nothing needs to remember" is true of the shard and not
quite true of the window.** The node's memory of which reset ids it has applied is in its process. That is safe
across a shard *move*, for the reason above, and not across a *restart inside the acknowledgement window*: a node
that applies a reset and restarts before the supervisor observes the acknowledgement will apply it again, rewinding
the consumer by whatever it consumed in between. The window is one of this service's passes.

It re-consumes rather than loses, because every ingested message must carry an `_id` — the data plane's message
processor rejects one that does not — so a replay re-indexes the same documents. The exception is a stream of
`_op_type: create` messages, where a replay is a version conflict per document and the error policy decides whether
that drops or blocks. Neither side keeps a durable ledger for this, which is a judgement rather than an oversight:
the cost is bounded and rare, and a ledger would have to be correct across exactly the crash it exists for.

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

## 22. Letting go of a deleted index

`ORPHANED_COPY` was raised by the differ from the first version of it and consumed by nothing. That reads like a
missing feature — disk not reclaimed — and it was three bugs.

### 22.1 One cause, three symptoms, and only one of them looks related

A node holding a copy of an index that no longer exists:

1. **keeps the copy for ever**, which is the obvious cost and the least of them;
2. **holds its deletion task open for ever.** `TaskCompletion` refuses to complete a `delete-index` while any
   node still holds or is assigned the index, which is correct — reporting the deletion done while a node serves
   the data would be a lie the control plane acts on. With nothing dropping the copy, the task retries to its
   attempt limit and becomes `TASK_STUCK`, one per deleted index, for ever;
3. **stops that node ever reporting an assignment as applied.** This is the expensive one, and nothing about the
   finding hints at it. `NODE_CONTRACT.md` has a node *defer* an assignment entry naming an index it cannot
   resolve, and report anything deferred as `outcome: failed` — the honest answer, and the right one. But §21
   completes a consumer-pointer reset only on an `APPLIED` acknowledgement. So one index deleted and recreated
   under the same name silently disables pointer resets on that node, permanently: the reset is handed over, the
   node applies it correctly, and the task ages out as stuck.

Symptom 3 is why this is a correctness fix rather than housekeeping. Two features that never mention each other
were coupled through a leftover document.

### 22.2 An assignment entry is a copy, for this purpose

The finding covers both a **held** copy and a bare **assignment entry** naming an absent index, as one kind with
one age.

Two kinds would have been the obvious split — a copy is data on a disk, an entry is a line in a document — and it
is wrong twice over. The entry is the more expensive of the two, per symptom 3 above. And the entry is the *only*
part of this a removal can actually withdraw: an assignment is the complete statement of what a node should hold,
so there is nothing else for the supervisor to say. Splitting them would mean two ages, two rules and two ways to
be inconsistent about one question.

Where a node both holds and is assigned the copy, **held wins** as the recorded reason. Not cosmetic: the age
must not depend on which of the two records the differ happened to visit first.

### 22.3 Positive evidence beats an absence

The question is never *"is this copy wanted?"* — it is *"do I know enough to be sure nobody wants it?"*, and
absence of evidence answers the first while looking like an answer to the second.

So `OrphanReaper` looks for a `delete-index` **task** naming the uuid. That is the control plane saying the index
was deleted, rather than the supervisor inferring it from a document it cannot find, and with it the copy is
withdrawn after the ordinary `minimumPasses`. Without it, the wait is thirty passes.

**The task's state deliberately does not matter.** What is being read is that a deletion was *requested*, which is
a fact about the control plane; a task's state is bookkeeping about this service's own progress. A task given up on
after five attempts is exactly what a pool full of unreapable orphans produces, so requiring a live task would
disable the cleanup in precisely the case that needs it.

This cannot mistake a live index for a deleted one, and the reason is an ordering rather than a check: a task
naming an index still in desired state raises no `ORPHANED_COPY` for the evidence to be applied to.

### 22.4 Two absences that must never be acted on

Both are cases where the supervisor is wrong about what exists, rather than cases where a cluster is unusual.

**A tenant with no index documents at all.** Indistinguishable from a failed read of that tenant's index prefix —
a prefix that moved, a layout the control plane changed, a deserialisation that started throwing — and the two want
opposite actions. So the copies are kept, indefinitely, and the finding is left to age. The cost is stated plainly:
a tenant whose *last* index was deleted, and whose deletion task the control plane has since pruned, holds that
copy for ever. Accepted, because only one of the two possible mistakes can be undone. A deletion task naming the
uuid is the escape hatch, and it covers the ordinary case.

**A tenant not bound to this pool.** A tenant rebound elsewhere looks exactly like a tenant whose indices were
deleted: its documents are absent because this supervisor no longer reads them. Those copies are that tenant's
data, being served by whichever pool owns it now.

Both refusals are written to `Plan.refusals` against the finding's own key, the same channel as a refusal to
*place* a copy (§16). Several of these never resolve on their own, and an unexplained finding sitting for ever is
how the previous version of this was eventually noticed — by someone reading a status document in confusion.

### 22.5 What the patience is calibrated for

Two passes is calibrated for a **torn read**: R2.5 promises the supervisor will observe an index without its
alias, and one more pass settles it. Thirty is calibrated for something else — a mistake in *reading* desired
state, which does not settle on the next pass because it is not a race. That class of mistake makes every index
look deleted at once, and at two passes of patience the pool's data is gone about ten seconds later.

Thirty passes does not make that safe and nothing here could. What it buys is that the operator who notices a
control plane behaving oddly has minutes rather than seconds, and that the diff fills with `ORPHANED_COPY`
findings each carrying a refusal that explains the wait, before anything is destroyed.

### 22.6 Still not covered

A copy **held with no assignment entry** is refused with a reason rather than acted on, because there is nothing
left for this service to withdraw — the document already says "do not hold this". Dropping it is the node's to do.
Planning a removal anyway would write no change and report a copy dropped, which is worse than leaving it.

## 23. A primary and a read-only copy competing for the last node

Found by running a real pool down to one node. The survivor held a shard's **search replica**; the node that held
its **primary** was long dead; `SameShardDecider` refused the only candidate for the primary because it already
held a copy of that shard. Every rule behaved exactly as designed, and the index could never be written to again —
not after a wait, not after a supervisor restart, not ever.

**It is not a tie.** Reads survive losing a copy. Writes do not survive losing their primary, and a search replica
**cannot become one**: upstream has no promotion path for a search-only copy, so the copy's presence is pure
obstruction rather than a head start. That asymmetry is the whole rule.

### 23.1 Ask the allocator again; do not read the refusal

The tempting implementation is to match on the refusal — kind is `UNASSIGNED_PRIMARY`, decider is `same_shard` —
and delete the copy. That guesses about the other eight deciders. A node refused for **disk as well as** for
holding the copy would lose a serving copy and gain nothing, which is strictly worse than the deadlock it was
meant to break.

So `Displacer` asks the question it actually means: it re-runs the allocator against a context where those copies
are treated as already gone (`AllocationContext.withoutCopiesOfThisShardOn`), and acts only if the primary is
placed *on the node whose copy would go*. Every decider runs unchanged; only the copy counts differ. If the answer
is still no, the reason is written into the refusal — because `same_shard`'s "already holds a copy" reads as
*"delete that and this would work"*, and it matters to know when that is false.

The scope is deliberately narrow in one more way. If removing the copy on A would let the primary land on **C**,
the obstruction was indirect — an awareness or address rule counting A's copy against C — and that is reported
rather than acted on. "Delete a serving copy on A so that C can take a primary" is a longer argument than this
rule has made.

### 23.2 Drop now, place later

Only the removal is planned. The primary is placed on a later pass by the ordinary path, once the node reports the
copy gone, and **nothing is remembered in between** — the next pass re-derives all of it from what it observes,
like every other decision here. The same shape as a move completing as a surplus (§14.5): two independent
decisions, neither aware that the other happened.

A node that has been told to drop the copy but still reports it is **waited for, not told again**. Re-planning the
removal every pass would write no change, spend a slot against the work ceiling, and claim in the plan to have
dropped a copy — so the wait is stated in the refusal instead.

**The one-write alternative is real and is the eventual upgrade.** An assignment holds one role per shard, so
changing `{0: search_replica}` to `{0: primary}` is a single field in a single document, with no window at all. It
needs the data plane to turn a read-only copy into a writeable one in place, including removing an index-level
`index.blocks.search_only`, so it is a coordinated change across two repositories. Dropping and placing needs
nothing from anybody. Its costs, stated: the copy is rebuilt from the remote store rather than reused, and the
shard serves no reads for a window. Both are better than a shard that can never be written to.

### 23.3 Why it cannot undo itself

Having given up the search replica, the pool is one node holding a primary and one unplaceable search replica —
which is the mirror image of the state that started this. The rule cannot run in that direction: it fires only for
`UNASSIGNED_PRIMARY`, so a primary is never given up for a replica. The resulting `UNASSIGNED_SEARCH_REPLICA` is a
yellow index with an explanation, which is the correct end state for a one-node pool.

## 24. Announced deployments — future work, not implemented

**Nothing here exists.** It is recorded because two or three of the decisions are much cheaper to make now than
to retrofit, and because one of them changes the value of a change already on the table (§23.2).

OpenSearch 3.x has a `_cluster/deployment` API: an operator — or, better, the deployment system itself — tells
the cluster that a set of nodes, usually identified by an attribute such as a pod or a zone, is going down and
will be back. Upstream already demotes primaries on those nodes gracefully; keeping replicas in place is done by
tuning the "node missing, move shards" timeout rather than directly. The intent is eventually to weight routing
traffic back up gradually, so a bad deployment has a smaller blast radius. The plan is for the standalone control
plane to grow the same API and to publish the resulting state for this service to read.

### 24.1 Why this is worth more here than the threshold tuning it replaces

A node is unreachable for one of three durations, and they are not a continuum:

1. **seconds** — a stop-the-world GC or a network glitch, possibly repeating;
2. **minutes** — a restart or a deployment;
3. **for ever** — dead, or missing long enough that if it returns it should be treated as a new node.

Every liveness threshold in this service is a compromise across all three. `stale` at 6× the interval is sized
for (1); `lost` at 20× is sized so that (2) does not trigger a rebuild, which is also why reaping waits thirty
further passes. **An announcement carves (2) out of the middle**, and the payoff is not only that deployments stop
causing churn — it is that the ladder gets to be *tighter at both ends* for everything that did not announce
itself. An unannounced disappearance is a much stronger signal once announced ones are labelled.

That is the argument for reading this signal rather than for continuing to widen timeouts: widening a timeout
makes the system slower at (3) in order to tolerate (2), and the two want opposite settings.

### 24.2 Three things to get right, and they are the retrofit-expensive ones

**It is a document with a deadline, not a notification.** An event would be edge-triggered, which R2.1 forbids
outright, and a supervisor that restarted mid-deployment would simply miss it. So it is desired state written by
the control plane, read every pass, and re-derived from scratch like everything else. This is not a small point:
a "deployment started / deployment finished" pair of notifications is the obvious API and it is the one shape
this design cannot consume.

**The deadline is load-bearing, not metadata.** A deployment system that crashes half way through must not leave
nodes announced for ever — announced nodes are nodes this service will decline to replace, decline to reap and
decline to place on, so a stale announcement is indistinguishable from a small permanent outage. An announcement
must therefore say *until when*, and an expired one must read as absent. Same discipline as a lease, for the same
reason.

**It is not a liveness state.** The temptation is a fifth `Liveness` value between `STALE` and `LOST`. That is the
same category error as counting an assignment as a copy: `Liveness` is what was *observed* — revision silence,
nothing else — and an announcement is what somebody *declared*. Keeping them orthogonal means an operator reading
the diff can still see that a node has genuinely gone quiet, and that the announcement is the reason nothing is
being done about it. Collapsing them hides exactly the thing worth seeing when a deployment does go wrong.

An attribute selector rather than a list of node ids, for the same reason the awareness rules take one: a
deployment system replacing pods does not know the node ids it is about to create, and `AllocationContext`
already resolves nodes by attribute.

### 24.3 Where it would be read, which is mostly in mechanisms that already exist

Recorded at this level of detail because the encouraging part is how little of it is new machinery:

- **`LivenessTracker`** — the `lost` threshold for an announced node becomes its announced window plus slack
  rather than 20× the interval. `stale` is unaffected and should be: a stale node's copies still count, which is
  precisely the behaviour wanted here.
- **A `DeploymentDecider`** refusing placement on an announced node, alongside the other nine. A node about to go
  away is a bad home for a new copy, and this is the same shape as `EnableAllocationDecider`.
- **`Differ`** — `NODE_LOST` on an announced node is expected rather than critical. The existing rule generalises:
  the alarm is that a finding is *old*, and here the alarm is that a disappearance was **unannounced**.
- **Nothing replaces its copies.** A node under deployment keeps counting toward redundancy, so
  `UNASSIGNED_REPLICA` is never raised for it — which is the half upstream does by timeout tuning and which falls
  out here for free, because copy counting already has a "does this count" question in it.
- **`Rebalancer`** — an announced node is not a move target, and a pool mid-deployment should not be rebalanced at
  all. Its heat readings are worse than useless during a traffic dial-up, which is the same argument §14 makes for
  not acting on a spike.
- **`NodeReaper`** — will not reap an announced node. And the inversion worth noticing: **an announcement that has
  expired makes reaping more confident, not less.** A node that was expected back in four minutes and is not back
  in twenty is more certainly dead than one that vanished without notice, so the aged-`NODE_LOST` wait could be
  shorter in exactly that case.
- **Weighted routing** lands on the projection rather than on placement: it is per-index routing state that
  coordinators read out of `tenant/<t>/status/indices/<uuid>`, not a decision about where copies live.

### 24.4 The one thing this changes about a decision already made

**Graceful primary demotion is the in-place role flip**, which §23.2 deferred as needing a coordinated data plane
change. Demoting a primary before its node goes down means rewriting one shard's role in one assignment —
`{0: primary}` becoming a read-only copy there, and a peer's copy becoming the primary — with no data movement at
all. That is the same capability §23's displacement wants in order to stop rebuilding a shard from the remote
store, and the same capability open question 1's fencing has to make safe.

So the in-place role change has three customers, not one. Worth knowing before anyone prices it as an
optimisation for a rare deadlock.

## 25. Two owners, not one

Vanilla OpenSearch assumes the party that **runs** a cluster and the party that **uses** it are the same person.
Every setting lands in one namespace, every API is available to whoever holds admin, and nothing in the model
distinguishes "I want this index" from "I own these machines". Here they are different parties, and after the
three-plane split itself this is the assumption with the widest reach — it decides, for any given setting or
request, which party owns it and therefore which plane may write it.

- **Tenants own the things they want to exist on OpenSearch**: indices, aliases, templates, data streams, search
  pipelines, mappings, and the flow of their own ingestion. Written through the control plane.
- **Operators own the machines and the timing of changes to them**: pools, node attributes and capacity, the
  remote store, deployment windows (§24), and the supervisor's own configuration.

### 25.1 The classifier

A setting is the **tenant's** if it describes *what they want to exist*. It is the **operator's** if it describes
*the machines, or the timing of changes to them*.

That is enough to settle open question 12 without appealing to anything else. `cluster.routing.allocation.awareness
.attributes` reads like a tenant preference and is a statement about where the **hardware** is: `zone` is a fact
about a rack in a building, which a tenant cannot know and did not pay for. So a tenant cannot declare it, and the
pool definition is the only place it can come from — which is what the code already does, for reasons that were
never written down until the control plane found the consequence.

### 25.2 Three categories, and the third is where the work is

1. **Tenant-owned.** The control plane writes it, the supervisor reads it.
2. **Operator-owned.** It lives in `pools/definitions/<pool-id>`, in a node's registration, or in the supervisor's
   configuration. No tenant can reach it.
3. **The intersection: the tenant states an intent, the operator states a capability, and the answer is the
   overlap.** This is the category that gets decided ad hoc if it is not named, and two instances already exist:
   - `index.number_of_replicas` against `IndexConfig.copiesAuthorisedFor` — the tenant configures, the operator
     authorises, and §15 keeps the two numbers separate precisely so per-shard autoscaling remains possible.
   - `index.replication.type: SEGMENT` against a pool whose nodes declare no segment repository —
     `RemoteStoreDecider` refuses the copy and says why (§17).

   Both resolve the same way: the tenant's request is honoured up to the operator's capability, and the shortfall
   is **reported rather than silently clipped**. Anything mixed that arrives later should take that shape.

### 25.3 The two failure directions want different remedies, and that asymmetry is the point

- A **tenant-owned** setting this service ignores is owed **a finding**. The tenant asked for something legitimate;
  silence is the defect `DESIRED_COPY_IGNORED` exists for, and `index.number_of_search_replicas` — read by nobody
  for as long as it existed, with a status claiming everything asked for had been delivered — is what it cost.
- An **operator-owned** setting a tenant writes should be **rejected at the control plane's boundary**. Reporting
  it later is strictly worse: the tenant finds out from a traffic split that never happens, or a shard that never
  moves, rather than from the request that asked for it.

Getting this backwards is easy, because a finding feels like the more generous answer in both directions. It is
not: a finding on a request the tenant was never entitled to make tells them they have a problem to fix, when what
they have is a request that should have failed.

### 25.4 Only the supervisor can enforce it

Structurally the same argument as §17.2's repository mismatch and §20.1's endpoint collision. The control plane
sees one tenant's desired state and nothing about the machines. A node sees itself and no other. **Only the
supervisor holds both the tenant's request and the operator's pool definition in the same pass**, so it is the only
component that can notice a tenant asking for something the operator has not provided — which is also why the
rejection in §25.3 needs this service to publish what the operator declared (`status/awareness`), rather than the
control plane guessing.

### 25.5 `_cluster/settings` is one namespace with two owners, and that predicts more of these

Open question 12 is the **first instance of a class, not a one-off**. Vanilla puts operator concerns and tenant
concerns in one flat `cluster.*` namespace with no marker separating them, so every such setting this system adopts
needs classifying — and an unclassified one silently lands wherever it was first implemented, which is what
happened here.

Two already in the code are worth re-examining against §25.1:

**`cluster.routing.allocation.enable` and `cluster.routing.rebalance.enable` are read from the tenant's settings**
(`ClusterSettings`, via `AllocationContext.allocationEnable`). Defensible, because the effect is confined to that
tenant's own indices and "stop moving my shards while I investigate" is a thing a tenant may legitimately want. But
the reason an operator reaches for these in vanilla is *"I am about to restart nodes"*, which is an operator intent
about the machines — and §24's announced deployments is where that belongs, not a tenant-scoped brake.

So the tenant-scoped reading is right and **incomplete: there is no operator-scoped equivalent.** An operator
cannot today say "stop allocating in this pool", and the nearest thing is asking every tenant bound to it to set
its own flag. Worth having, and it is a pool definition field rather than a cluster setting — which is the whole
point of this section.

## 26. Fencing a primary: what the path prefix does and does not buy

> **Partly superseded by §27.** The remote segment store already orders writers by primary term, so the per-primary
> path prefix proposed here is unnecessary (§26 intro, §26.5). §26.2's exclusion rule still stands for push
> ingestion; §26.3 and §26.4 stand with the primary term as the number. §27 also covers what a rollback needs,
> which this section did not.

Open question 1 is the largest gap here, and there is a concrete proposal for it: the
`index.remote_store.segment.path_prefix` setting — added upstream in response to an issue filed during the
`cluster-etcd` work — with a unique value per primary incarnation, so a new primary writes somewhere the old one
was not. Search replicas are told the same value and are rolling-replaced onto the new prefix when it changes.

The idea is sound and this section is mostly about what has to go with it, plus one change to who chooses the value
and one cheaper way to ask upstream for the hard part.

### 26.1 Isolation is not exclusion, and that distinction is the whole problem

**A unique prefix per writer prevents collision. It does not prevent two writers.** Two nodes each believing they
hold the primary, each writing to its own prefix, produce no corruption at all — and a **silent split of the
data**: some documents land under `P0`, some under `P1`, the search replicas follow whichever prefix they were
told, and the other half is simply invisible. Nothing reports it, nothing repairs it, and both nodes report
themselves perfectly healthy. That is worse than the collision it was meant to prevent, because the collision is
at least detectable.

So the prefix belongs to the *recovery* half of the problem — it is what makes a primary move cheap and an upgrade
rollback possible — and something else has to do the *exclusion*. It is worth being explicit that these are two
mechanisms, because a scheme that only isolates looks finished.

### 26.2 The exclusion half can be built now, with no upstream change

**A node holding a primary stops accepting writes if it has not re-read its own assignment within T.**

That is the whole rule, and every piece of it already exists. The node polls its own assignment key. It knows its
heartbeat interval. The supervisor already decides a node is gone from revision silence — so this is that same
mechanism pointed the other way: instead of the supervisor concluding the node is gone, the node concludes it may
have been *declared* gone. Set the node's self-demotion window below the supervisor's `lost` threshold and the
window in which two nodes can both believe they are primary closes by construction:

```
node stops writing at   T_self  = lost − margin
supervisor may reassign  T_lost
```

Three things make this cheaper here than it sounds:

- **The data plane needs no new capability.** Self-demotion is the node behaving as though its assignment had
  stopped naming it as primary — which is a thing it already does, every time the supervisor withdraws a copy. It
  needs a timer, not a new code path for refusing writes.
- **It needs no remote-store support and no OpenSearch change.** It is a contract rule and a clock.
- **It composes with the prefix rather than competing.** Exclusion makes the split impossible; the prefix makes the
  handover cheap. Neither substitutes for the other.

The cost, stated plainly: a node partitioned from the store stops serving **writes** for its primaries while
remaining up. That is the correct trade for a primary and it is a real availability cost, and it is the same trade
every lease-based system makes. Reads are unaffected, which matters — a search replica has no exclusion problem at
all, because it writes nothing.

### 26.3 The supervisor should choose the value, not the node

The proposal has the node hosting the primary inject a unique value — its node id or ephemeral id. In vanilla
OpenSearch that is the only option, because nothing else is in a position to. Here it is the weaker choice:

- a search replica has to be told which prefix to follow, and if the node picks it, the supervisor learns it only
  by reading it back — so the instruction depends on an observation of the thing it is instructing;
- and two nodes that both believe they are primary would each pick a value with **nobody arbitrating**, which is
  exactly the case the mechanism exists for.

The supervisor is the single writer of the assignment, so it is the natural allocator — and allocating the value
with a compare-and-set **is** the fencing primitive, rather than being a separate thing bolted beside one. A node
that finds a generation in its assignment newer than the one it is writing under knows it has been superseded
without being told.

### 26.4 Where the generation lives is the interesting consequence

It must be **monotonic per shard and durable independently of any node's documents**, and that is a sharper
constraint than it looks. `nodes/assignment/<id>` cannot be its home: §20.2's reaping deletes that key, and finding
10's lesson generalises — *a counter that restarts on deletion cannot be compared by ordering across the
deletion.* A projection cannot be its home either, because projections are derived and this is not.

So it wants its own supervisor-owned key, incremented under compare-and-set. And that makes it **the first
authoritative, non-derived state this service would own.** Everything the supervisor writes today is either derived
from an observation or an instruction computed from one; R2.1 is satisfied because a restarted supervisor can
recompute all of it. A generation counter is neither: it is *input* to a decision and cannot be recomputed from
what it can see.

That is a real departure and should be taken deliberately rather than discovered. It does not violate R2.1 so much
as add a second category — and the property that keeps it honest is that the counter only ever increases, so losing
track of it can only ever be repaired by skipping forward, never by reusing a value.

### 26.5 The shard-versus-index setting problem, and a ladder out of it

The known difficulty: the prefix is an **index** setting, and what is wanted is a **shard** setting, because one
index's shards have primaries on different nodes and therefore different generations. Shard-level settings are not
an OpenSearch concept, and asking for one is asking for a new concept.

Three steps, each shippable alone, each correct, in increasing order of what it asks of other people:

1. **Index-level generation, bumped for every shard at once.** Any primary move anywhere in the index advances one
   number, so generations never differ within an index and today's index setting is sufficient for ever. Needs
   nothing from anybody. The cost is real and bounded: a single primary move re-replicates the whole index. For a
   deployment where primary moves are rare — and a demo, and any early production — that is a price worth paying
   to have the correctness now.
2. **Per-shard generation, plus a placement rule.** Generations may then differ within an index, and the only thing
   that breaks is *one node holding two shards of one index whose generations differ*. That is an allocation
   decider — refuse a copy whose generation disagrees with a copy of the same index already on that node — which is
   exactly the shape this codebase is already built around, and it costs placement flexibility rather than
   correctness. It binds only after a primary has actually moved, so on a converged pool it is free.
3. **Ask upstream for an affix setting, not a shard setting.** `index.remote_store.segment.path_prefix.<shard-id>`
   as a grouped or affix setting is expressible in today's index metadata, and OpenSearch has plenty of precedent
   for settings keyed by something — `index.routing.allocation.require.*` among them. That is a keyed map rather
   than a new level of the settings hierarchy, which is a much smaller ask than "shards can have settings" and
   stands a far better chance of landing.

Step 2 is the one worth aiming at, and step 1 is worth building first because it is the same supervisor-side
machinery with the generation scoped differently — the seam is where the number is stored, not how it is used.

### 26.6 The rolling replace is already expressible

Flipping search replicas onto a new prefix needs no new instruction. "Replace a search replica" is remove one, place
one, and §14.5's add-before-remove ordering plus `SurplusReaper` already sequence exactly that — the machinery
§23's displacement uses. So the expensive-sounding half of the proposal is the half that is already built.

### 26.7 Three customers for the same mechanism

Worth noting before anyone prices this as fencing alone. A per-shard generation carried in the assignment is also
what §24.4's graceful primary demotion needs, and what an upgrade-with-rollback needs — a new primary on a new
OpenSearch version writing a new path, with the old path intact to roll back to. Same number, same channel, three
reasons to want it.

---

## 27. The primary term is already the generation, and what a rollback still needs

§26 set out to give each primary incarnation its own remote path. Reading the remote segment store's code — prompted
by the data plane's `WORKING_NOTES.md`, *"The primary term already orders the remote segment store"*, and checked
here against the source rather than taken second-hand — shows the store already orders writers by primary term.
That removes most of §26's machinery. It does not remove the rollback problem, which turns out to need three things
§26 did not identify.

### 27.1 What the store already does

- **The newest term wins on every read.** A segment metadata filename begins with `RemoteStoreUtils.invertLong(primaryTerm)`,
  then the generation (`RemoteSegmentStoreDirectory.MetadataFilenameUtils.getMetadataFilename`), and
  `readLatestMetadataFile` lists them lexicographically and takes the first. A superseded primary's metadata can
  never be read as the latest.
- **Two writers at one term fail loudly.** The node id is in the filename, and `RemoteStoreUtils.verifyNoMultipleWriters`
  throws when two node ids share a (term, generation). It inspects only the newest `METADATA_FILES_TO_FETCH` (10)
  files, so it is a tripwire rather than a guarantee.
- **Cleanup cannot delete the winner's data.** `deleteStaleSegments` deletes a data file only when a metadata file
  being deleted references it *and* no kept or locked metadata file does (`activeSegmentRemoteFilenames`).
  Uploaded data files are named with a uuid suffix, so two writers never produce the same blob. Even a superseded
  primary running cleanup — which it does here, because the upstream fence that would stop it
  (`isRemoteStoreFenceSupersededFailingClosed`) is off — leaves the higher term's files alone.

**So D8.2's "two primaries could write to the same remote path and corrupt it" is wrong for segments.** With both
primaries writing to one path, readers deterministically prefer the higher term and nothing is overwritten.

### 27.2 What that removes from §26, and what it does not

- **The path prefix is unnecessary** (§26 intro, §26.5). The primary term is already a per-shard value, already in
  `IndexMetadata`, already in the layout. The shard-versus-index-setting ladder does not have to be climbed, and
  nothing needs to be asked of upstream for isolation.
- **§26.3 and §26.4 survive with a different number.** The supervisor should still choose the value and it should
  still be monotonic per shard and durable independently of any node's documents. The value is the **primary
  term**, which finding 11 already requires in the assignment for promotion to work at all. One number serves
  promotion, fencing and rollback.
- **§26.1's warning changes form rather than going away.** A shared path means a lower-term primary's writes are
  simply invisible to readers. Under push ingestion, a lower-term primary that is still accepting writes loses them
  silently — the same split §26.1 described, arrived at differently. **§26.2's self-demotion rule is still required
  for push ingestion and document replication.** Under pull-based ingestion both primaries consume one stream, so
  running two is not a split but a feature, and that is the case the rest of this section is about.
- **Today every term is 1.** Two primaries of one shard therefore collide at the same term and hit
  `verifyNoMultipleWriters`. Finding 11 — the supervisor owning and advancing a per-shard term — is the
  prerequisite for everything below.

### 27.3 What a rollback needs

The deployment this design wants, which vanilla OpenSearch never does: start a new primary at term N+1 beside the
old one at N, move search replicas across gradually, and be able to move them back if the new version misbehaves.
Three facts of the replication and cleanup paths stand in the way.

**Search replicas cannot be pinned.** `RemoteStoreReplicationSource.getCheckpointMetadata` reads through
`remoteDirectory.init()`, which is `readLatestMetadataFile`. So the first upload at N+1 moves every search replica at
once. That is right for failover and rules out a gradual rollout. Pinning needs an upstream change, and the
narrowest one is:

> a search-only copy reads the newest metadata whose term is **≤ the term its cluster state names for that shard**.

The data plane's note worried about the term field carrying two meanings. Under this rule it carries one: a copy
never reads from a primary its cluster state has not told it about. That is defensible upstream by itself, independent
of this deployment. **The term must be read from `IndexMetadata.primaryTerm(shardId)`, not from the shard.** A
shard's own term is set only at construction, on promotion (`updateShardState`, primary branch only) or when a
primary's operation arrives (`acquireReplicaOperationPermit`), and a remote-store search replica receives no
operations — so its own term never moves. The same fact makes raising a search replica's fabricated term in place
harmless: `updateShardState` does nothing with a term change on a non-primary entry.

**A replica can move forward in place, but not back.** `SegmentReplicationTargetService` starts a round only when the
received checkpoint `isAheadOf` the replica's own, and that comparison is term-first. The check is skipped only
while the copy is initializing. So raising a replica's pin from N to N+1 is in place; lowering it is a **rebuild** —
remove the copy and place a new one pinned to N. §26.6's point stands in the asymmetric form: remove-then-place
already exists (§14.5, §23), and rollback is the direction that needs it.

**Cleanup deletes the old primary's state within seconds.** `deleteStaleSegments` keeps the newest
`cluster.remote_store.index.segment_metadata.retention.max_count` files (default 10) across **all** terms, and
every term-N+1 file sorts ahead of every term-N file. So about ten refreshes after the new primary starts, all of the
old primary's remote metadata — including what it uploaded a moment ago — is eligible for deletion, and so are the
data files only it references. With pinned timestamps disabled, `filterOutMetadataFilesBasedOnAge` adds no grace.
Both primaries run cleanup, so the old primary deletes its own history too. A replica pinned to N reads files that are
being deleted underneath it.

Three ways to spare term N, in increasing order of precision:

1. **Turn cleanup off on the two nodes for the duration.** The retention setting accepts `-1`, which makes
   `deleteStaleSegments` return immediately (`0` is rejected upstream as "would delete all the data"), and it is a
   dynamic *cluster* setting — so each node can carry it in its own fabricated cluster state with no upstream change.
   **Confirmed end to end by the data plane**: a persistent setting in fabricated metadata reaches
   `ClusterApplierService.applySettings`, then `RemoteStoreSettings`' update consumer, and
   `RemoteStoreRefreshListener` reads the value live on each sync, so it takes effect on running shards.
   It did not work until the data plane fixed its own state comparison, which looked only at indices and routing
   tables: a state differing only by a cluster setting compared equal, was never applied, and the node would have
   *said* cleanup was off while cleanup ran. The general rule it left behind — anything a node puts in a cluster
   state outside an index must be in that comparison or it does not happen — is worth remembering for every
   instruction that follows.
   **The cost is cross-tenant.** The setting stops cleanup for every primary on the node, not only the shard being
   deployed, and a shared pool (§2) puts several tenants on one node. So one tenant's deployment grows other tenants'
   remote storage. Placement cannot fully contain it: the new primary's node can be chosen, but cleanup must also
   be off on the *old* primary's node, which is wherever the old primary already is.
2. **Lock the old primary's metadata** with `RemoteStoreMetadataLockManager`, the mechanism snapshots use and that
   cleanup honours by failing closed. Precise, but a lock pins one file and the old primary uploads a new one every
   refresh, so the lock would have to follow its head.
3. **Pinned timestamps**, which cleanup also honours as implicit locks. Not investigated.

**Recommended: (1) for dedicated pools, (2) for shared ones.** In a dedicated pool every shard on the node belongs to
the tenant being deployed, so (1)'s blast radius is that tenant's own storage for the length of its own deployment,
and it needs nothing from anybody. In a shared pool the cost falls on tenants who asked for nothing, which is the
argument for the lock — scoped to one shard's files by construction — despite its having to follow the old primary's
head. The pool kind is already in `PoolDefinition`, so the supervisor can choose per deployment rather than per
fleet.

Either way the lifecycle has the same shape: **term N's metadata being spared means rollback is still possible;
releasing it commits the deployment.**

### 27.4 Rollback never demotes the old primary

The obvious model — "fail back to the old primary" — cannot work, for two reasons found in `IndexShard.updateShardState`:

- terms only go up (`assert newPrimaryTerm > pendingPrimaryTerm`), so the old copy cannot return to term N as
  primary — it would have to take N+2;
- and a started primary cannot take a higher term in place (`assert currentRouting.primary() == false : "term is only
  increased as part of primary promotion"`), so it would have to be re-opened — and re-opening a remote-store copy
  runs `syncSegmentsFromRemoteSegmentStore`, which, as the data plane's notes document, replaces local commits with
  the **latest uploaded** one. That is the new primary's: the state being rolled back from.

So the old primary must simply never stop. The sequence:

1. **Deploy.** Place a new primary at term N+1 beside the old one at N. Cleanup off on both nodes. Search replicas
   stay pinned to N.
2. **Roll forward.** Raise search replica pins to N+1, one at a time, in place.
3. **Roll back.** Rebuild each moved replica pinned to N, retire the N+1 primary, and **delete term N+1's metadata**.
   Leaving it is not harmless: once cleanup is back on, the old primary's keep-window would retain the dead N+1
   files ahead of its own and delete its own current state.
4. **Commit.** All replicas at N+1: turn cleanup back on, retire the old primary.

This works because pull-based ingestion keeps both primaries consuming one stream, so the old one is never behind
and no data has to flow back to it.

### 27.5 What is still open

- **The pin is an upstream change**, and the only one this section needs. Everything else is supervisor and data
  plane work.
- **Deleting term N+1's metadata on rollback needs repository access**, which the supervisor does not have (the same
  reason R5.3 is deferred). The natural actor is a data plane node, instructed through its assignment.
- **The instruction vocabulary** is designed in §28: one per-copy term that also serves as the pin, retention as
  per-copy intent with the mechanism declared by the node, and term abandonment as an acknowledged instruction. The
  data plane side of the cleanup switch is done; nothing yet asks for it.
- **A lock that follows the old primary's head** (27.3's option 2) is undesigned. It is the one that shared pools
  want, and the obvious holder is the old primary itself — it knows each file as it uploads it — told through its
  assignment that it is a rollback target.
- **Push ingestion and document replication** get §26.2's exclusion rule and not this section's dual-run. Two
  primaries accepting writes is a split, whatever the path layout. An upstream alternative exists and is nearly
  reachable: the translog-side fence (`IndexShard.sealRemoteStoreFence`, a compare-and-set token) whose enforcement
  already reads ownership on the segment path, but which is claimed on the translog repository and so is gated off
  in `segments_only` mode. The data plane has reported that gap upstream; if it closes, exclusion for push ingestion
  may come from OpenSearch rather than from a self-demotion timer.

---

## 28. The assignment fields for terms, pins and retention

**Status: partly implemented.** §27.5 listed the instruction vocabulary as open; this is it. The wire format is
in `NODE_CONTRACT.md`, *Proposed: primary terms and remote-store history*. This section is the reasoning.

**Built on the supervisor side:** `contract_features` (§28.3), `primary_terms` on every assignment (§28.1–28.2),
the counter (§28.6), and promotion (§28.7's first sequence, finding 11) — `TermLedger`, `Promoter`. **Not built:**
retention (§28.4), abandonment (§28.5), and the dual-run. **Not built on the data plane side:** any of it, so until
a node declares `primary_terms` no primary is promoted and every node carries on at term 1.

Three things learned building it, each recorded where it applies:

- **Promotion was also a data-loss fix.** Under document replication a lost primary used to be replaced by a
  fresh, empty primary elsewhere — the allocator refuses the replica's node because it already holds the shard —
  and the surviving replica would then recover from it. `Promoter` makes the replica the primary where it stands,
  and when it cannot (the node does not declare `primary_terms`) it **refuses** rather than letting the allocator
  place an empty one. A shard that cannot take writes is recoverable; a wiped replica is not.
- **A promotion in flight must not be repeated.** A node reports what it holds, and that beats what it was told,
  so a replica told `PRIMARY` still reads as a replica until it applies the change. Without a guard the planner
  would promote again every pass and advance the term each time.
- **A new index's first primary is at term 2**, because the counter is created at 1 and placing a primary always
  advances it. No special case for "first": a shard with no primary cannot be told apart from one whose primary is
  on a node now lost, and that second case is exactly the one the new term fences.

Also not tracked, and worth knowing before relying on promotion: the in-sync set. OpenSearch promotes only an
in-sync replica; `Promoter` promotes any started replica on a node still heartbeating.

**And a returning old primary is withdrawn, not re-stamped.** Found after the data plane implemented its side: a
node lost while primary keeps its assignment unrewritten, because only placeable nodes are visited, so when it came
back its stale `PRIMARY` entry was stamped with the *new* term — two nodes told they were primary at the term that
exists to tell them apart. The entry carries its own evidence: a `PRIMARY` entry recorded below the shard's current
term, which this pass is not placing, has been superseded, and the applier drops it (`dropSupersededPrimaries`).
While the returning node still holds its old copy, `PRIMARY_CONFLICT` reports two primaries; the differ used to ask
only whether a shard had none.

Four fields and one registration field. The design choices that matter are why there are only four.

### 28.1 The term and the pin are one field

Each copy gets one number: **the primary term this node's cluster state names for that shard.** What it means
follows from the copy's role, and every meaning is one OpenSearch already gives the term:

| role | what the term does |
|---|---|
| `PRIMARY` | the term it writes under. An increase on a copy that was not primary is a promotion (finding 11). |
| `REPLICA` (document replication) | the term it accepts operations at; a primary writing at an older term is refused. |
| `SEARCH_REPLICA` | the highest term whose remote metadata it reads — the pin, once upstream honours §27.3's rule. |

A separate pin field was the obvious design, and it would be two numbers that can disagree, leaving a node to decide
which wins. Under §27.3's rule — *a search-only copy reads metadata no newer than the term its cluster state names* —
they are the same number, so they are one field. Until upstream implements that rule, a search replica ignores it
and reads the latest, which is today's behaviour. The field is therefore safe to ship before the upstream change,
and becomes a pin when it lands.

Terms are per copy rather than per shard because each assignment is one node's. In steady state every copy of a
shard is told the same term. The exception is §27.4's dual-run, where the old primary is told N and the new one
N+1 — which is why the field cannot live in a shard-level document.

### 28.2 A sibling map, not an object

`shards` stays `{"0": "PRIMARY"}`. The terms go in a sibling map, `primary_terms: {"0": 4}`, on the same index entry.

Making each shard value an object would say it more directly, and it would break every deployed reader in the
direction that matters: a supervisor upgraded before its nodes would write documents no node can read. The `roles`
change (§18.1) cost a running deployment in exactly that way, and a sibling map is additive — an older reader ignores
it and carries on at term 1, which is today's constant.

The cost is that two maps can disagree, so the rules are stated rather than assumed: the supervisor writes a term for
**every** shard in `shards` or for none of that index, never some; a shard with no term is at 1; a term for a shard
absent from `shards` is an error the node defers with a reason.

### 28.3 A node must say which fields it understands

The ingestion reset is acknowledged through `applied` (§21.2), and that works because a node that did not understand
the reset would not be holding the shard's primary consumer anyway. **For terms, `applied` lies.** A node whose reader
ignores an unknown `primary_terms` field applies everything else, fabricates term 1, and reports `applied` — and for a
promotion that means recreating the shard at the same term as the old primary, which is two primaries at one term.

So registration gains **`contract_features`**: a list of strings naming the optional parts of this contract the node
implements. Written once per process, 4 KiB budget, no per-copy cost. The supervisor:

- writes `primary_terms` to every node regardless — harmless to one that ignores it;
- **only moves or dual-runs a shard's primary when every node involved declares `primary_terms`**. In a pool where
  some node does not, primaries stay where they are, which is today's behaviour. Nodes can therefore upgrade one at
  a time with no flag day;
- ignores feature strings it does not recognise, for the same reason a node ignores unknown fields.

The inventory gains nothing. It is at 89% of its budget with no room for a per-copy field (the contract's own
measurement), and with capability declared up front, `applied` plus the existing deferred reasons are enough to
confirm a term took effect.

### 28.4 Retention: intent per copy, mechanism per node for now

`retain_remote_history: {"0": 3}` on a `PRIMARY` copy means: **spare shard 0's term-3 remote metadata, and the files it
references, from cleanup.** During a dual-run both primaries carry it, with the old term as the value, because both
run cleanup (§27.3).

The intent is per copy even though today's mechanism is per node, and that corrects §27.5's first draft, which put the
field on the node because that is the setting's granularity. Stating intent lets the mechanism change without a format
change:

- **`retain_remote_history`** — the node implements it with the `-1` retention setting, so every primary on the
  node stops cleaning up;
- **`retain_remote_history_scoped`** — the node implements it with a metadata lock that follows the old primary's
  head, so only that shard is affected.

The supervisor knows the cost and makes the choice, per §27.3: **a dedicated pool accepts either feature; a shared pool
requires the scoped one**, so no tenant's deployment grows another tenant's storage. The same field, two feature
strings, and the policy stays where the knowledge is.

### 28.5 Abandoning a term is an instruction, acknowledged like a reset

`abandon_remote_terms: {"id": "<rollback id>", "shards": {"0": 3}}` means: **delete shard 0's remote metadata whose term
is above 3.** It is §27.4's rollback step, and it is an instruction rather than desired state for the reason §21.2
gives: expressing it as a standing condition ("no metadata above term 3 exists") would have the node list the
repository for every shard on every pass. It runs once, keyed on its id, and is acknowledged through `applied` exactly
as a reset is.

It is destructive, so the supervisor writes it only when all of these hold:

- no observed inventory and no live assignment holds a copy of the shard at a term above T — the newer primary has
  been retired **and has gone**, or it would upload new metadata behind the deletion;
- the receiving copy is the shard's `PRIMARY` at term T;
- the receiving node declares `abandon_remote_terms`.

And it must be acknowledged **before** retention is released. Released first, the old primary's cleanup would keep the
abandoned term's files ahead of its own and delete its own current state (§27.4).

### 28.6 Where the term comes from

A supervisor-owned key, `supervisor/terms/<tenant>/<index-uuid>`, holding the highest term ever issued for each
shard. It is §26.4's counter with the primary term as its value, and it is the first authoritative, non-derived state
this service owns.

- **Incremented under compare-and-set before any assignment carries the new term.** An assignment written first and the
  counter lost after would reissue the term.
- **Created with the index, at 1 for every shard.** So an absent counter for an index this service has seen is an
  error, not a default: the supervisor refuses to move that index's primaries and raises a finding, because
  guessing 1 after terms have advanced is the reuse finding 10 warned about. Indices that predate this get their
  counter when first observed; that is the one moment absent means "never advanced".
- **Tenant- and index-scoped, not pool-scoped**, so it survives a pool handover. Deleted with the index uuid, which
  is safe because the remote store path is per index uuid — a recreated index starts from an empty path.
- **Outside `tenant/<t>/`**, because the control plane has no business reading or writing it.

### 28.7 The two sequences, as field changes

Promotion — document replication, or a drain (finding 11). One pass:

1. Counter for the shard: N → N+1, under compare-and-set.
2. The copy being promoted: role `PRIMARY`, term N+1.
3. The demoted copy: role `REPLICA`, term N+1 — in the same pass, so it sees it has been superseded rather than
   refusing its own writes.
4. Every other copy of the shard: term N+1.

Dual-run deployment with rollback (§27.4), across many passes:

| step | fields |
|---|---|
| deploy | counter N → N+1; new copy `PRIMARY` term N+1 with `retain_remote_history` N; old copy stays `PRIMARY` term N, gains `retain_remote_history` N; search replicas stay at N |
| roll forward | one search replica at a time: term N → N+1, in place |
| roll back | each moved search replica removed and replaced at term N; new primary removed; once it has gone, `abandon_remote_terms` N on the old primary; once acknowledged, `retain_remote_history` removed |
| commit | all search replicas at N+1; `retain_remote_history` removed from both; old primary removed |

### 28.8 What each side validates

The node defers, with a reason, rather than acting on:

- a term for a shard not in `shards`;
- a term lower than the one a started copy is already running — the supervisor should replace the copy instead;
- `retain_remote_history` or `abandon_remote_terms` on a copy that is not `PRIMARY`;
- `abandon_remote_terms` whose T is not the receiving copy's own term.

The supervisor never writes a term below the counter, never writes two `PRIMARY` copies of a shard at one term, and
never lowers a copy's term in place.

### 28.9 What this does not decide

- **Where "a deployment is in progress for this shard" lives.** Two `PRIMARY` copies of one shard are, today, a
  surplus. The differ and the reaper need to know a dual-run is intended, and that is desired state, not an
  assignment field — the announced-deployments document recorded as future work is its natural home.
- **The upstream pin.** Until it lands, a search replica's term is recorded and has no effect on what it reads, so
  the roll-forward and roll-back steps move nothing.
- **The scoped lock's design** (§27.5).
