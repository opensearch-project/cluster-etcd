# Monitoring and alerting

The supervisor already detects nearly everything worth alerting on. What it cannot do is decide that a
detected thing has gone on long enough to deserve a human, and it cannot tell anyone. This document is about
that gap: what the alerter is, why it is not part of the supervisor, and the rules it evaluates.

Written before any code exists, in the same spirit as `REQUIREMENTS.md`. The load-bearing claim is that the
rules are worth more than the process that runs them, so the rules come first and the process is optional.

---

## 0. The decision, and what it costs

**Detection stays in the supervisor. Judgement and delivery leave it.**

Three stages, in order, and each is useful alone:

| stage | artifact | needs |
|---|---|---|
| 1 | a rule library, and `supervisor-app alerts` — one shot, read-only, exit status | nothing new |
| 2 | four fields the projections do not yet carry | small supervisor changes |
| 3 | a long-running `opensearch-monitor` that runs the same rules and notifies | a lease, an outbound seam |

**Stage 1 is the whole point of this document.** A one-shot command that prints what is wrong and exits
non-zero is enough to run from `cron`, from a CI job, from a terminal during a demo, and it is the same code
a daemon would later run in a loop. Stages 2 and 3 are named so that stage 1 does not foreclose them.

What this costs, stated plainly: an alerter reading projections is one step removed from the reasoning that
produced them, so **every alert is only as good as the `because` the supervisor wrote down.** Where that
string is missing or vague, the fix is in the supervisor, not here. §9 makes that a rule rather than a hope.

**Written alongside the §20.2 node-reaping work**, and it assumes that work lands. Two rules depend on it
directly — `REGISTRATION_REWRITTEN` in §5.2 and §7.5 — and the dependency runs the interesting way round: a
mechanism that repairs itself needs *more* monitoring than one that fails visibly, not less.

---

## 1. Why the alerter is not part of the supervisor

The obvious implementation adds a thresholds file and a webhook to the process that already has every
number. Three properties of this service make that the wrong shape, and two of them are invariants rather
than preferences.

### 1.1 R2.3 forbids the memory that leak detection needs

R2.3: *"There must be no state held only in memory whose loss leaves the system stuck."* `FindingAges`
honours it exactly, and says so — *"Held in memory and bounded… Losing it on restart is safe and required to
be."*

Two consequences follow, and together they decide this question:

- **Ages are counted in passes, not wall-clock**, so they are not comparable across processes and do not
  survive a restart. `DESIGN.md` §20 records the same property for liveness: every restart grants every
  corpse in the pool a fresh staleness window.
- **A cleared finding loses its history on purpose**, so that the same problem recurring is a new problem
  rather than an apparently ancient one. That is right for planning and wrong for noticing: a finding that
  clears and returns forty times is the most interesting thing in the pool and leaves no trace.

So the supervisor's horizon is one process lifetime, measured in passes. A leak that becomes visible over
weeks cannot be seen from inside it. This is not an omission to patch; extending the supervisor's memory to
cover it means arguing with R2.3.

The clean division is worth stating as a sentence: **correctness must not depend on memory; noticing may.**
Losing the alerter's memory delays an alert. Losing the supervisor's misplaces a shard. Those are different
risks and they belong in different processes.

### 1.2 A pass cannot report its own absence

Three documents in this repository say a version of the same sentence. `REQUIREMENTS.md` §7: *"A reconciler
that is quietly failing looks exactly like one with nothing to do. Both produce no output."* `FindingAges`
repeats it. `REQUIREMENTS.md` §8 says it about watches: *"a silently dead watch looks exactly like a quiet
keyspace."*

Every observability document in `SUPERVISOR_KEYS.md` §4 is written **by a pass**. A wedged supervisor, a pool
whose lease nobody took, a process that died between two steps — all of them present as documents that
simply stop changing, and nothing raises a hand. An alerter hosted inside the supervisor is structurally
unable to fire the most important alert it has.

`SUPERVISOR_KEYS.md` §4 already made this call once, for the same reason, about a different mechanism:
serving the diff over HTTP was rejected because it *"would be unreachable in exactly the situation where it
is wanted — a wedged process."* Hosting the alerter in the supervisor reintroduces the flaw that reasoning
removed.

### 1.3 Alerting is outbound I/O, and the supervisor has exactly one seam

`REQUIREMENTS.md` §1: *"No plane calls another directly."* The control plane holds to it with one outbound
seam, `KeyValueStore`, and the supervisor does too. `supervisor-model` cannot see `store-api` at all, and
nothing in the repository names etcd or ZooKeeper at compile time.

A webhook client, an SMTP client and a set of credentials in the process that writes shard assignments costs
all of that. Worse, it couples the two: a notification endpoint that accepts a connection and never responds
turns a paging outage into a shard-placement outage, in the pass loop, at the moment the cluster is already
unhealthy.

**The monitor is the component where dependencies are affordable.** A Prometheus or OTLP client belongs
there and nowhere else, and isolating it is a reason to have the component rather than a cost of having it.

---

## 2. What stays in the supervisor, and why it must

Being fair to the alternative, because two of these are real arguments for the other side:

- **Only the supervisor can attribute a cause.** `DESIGN.md` §16's one-example-per-distinct-reason grouping,
  the refusal channel `Planner` and `OrphanReaper` write, `Finding.because` — none of it is re-derivable from
  raw keys without reimplementing the differ. `REQUIREMENTS.md` §9 forbids a second source of truth, and §8
  gives the general form of the argument about store code: two implementations of one semantics is how the
  planes silently disagree.
- **Some checks are only possible from inside.** `DESIGN.md` §20.1 and §17.2 both turn on the supervisor
  seeing every node's documents at once, which no node and no reader of a projection can.
- **Detection is already good.** Findings with severities, ages, `because`, convergence `trend`,
  `unpooled_tenants`. Very little is missing. §7 lists what is.

  **Not the oscillation ring, which does not exist.** `Keys.poolOscillation` is a key builder with no caller, so
  R7.3's detector is unimplemented and the document `SUPERVISOR_KEYS.md` §4 describes in detail is never written.
  An oscillation shows up today only as a diff that never empties — the alarm without the explanation.

One consequence of §20.1 deserves to be called out, because it is the cleanest example of why this document
exists. The node projection publishes only the freshest registration per `address:port`, which was the right
fix for the symptom an operator hit. It also means the count of superseded registrations — the leak itself —
now exists only inside a pass and is discarded at the end of it. **The fix hid the leak from every reader.**

---

## 3. The seam: rules are a pure function of projections

`DESIGN.md` §1 claims everything is a pure function except two edges, and the module graph makes the claim
checkable: `supervisor-model` cannot see a `KeyValueStore`, so it cannot quietly read a key it forgot to
observe. The rule library takes the same shape and inherits the same guarantee.

```
Reader (supervisor-core)     scanPrefix over supervisor/status/**, supervisor/members/**,
                             pools/definitions/**, and key counts under nodes/** and tasks/**
        |
        v
Snapshot (supervisor-model)  parsed projection records + a wall-clock instant + the previous Sample
        |
        v
Rules (supervisor-model)     pure: Snapshot -> List<Alert>.  No store, no clock, no I/O.
        |
        v
Sink                         stdout and exit status (stage 1) | notifier and gauges (stage 3)
```

Because the middle is pure, **where it runs is a deployment decision made later.** The same library can be
called by `supervisor-app alerts`, by the monitor daemon, or by a test with hand-built inputs. That is what
makes the binary choice in the title a false one, and it matches how the differ and the planner are already
tested — `WORKING_NOTES.md`: put the risk in code that needs no network.

### 3.1 The reader half does not exist yet

`Json`'s own javadoc anticipates this use — *"reads a projection — so that there is one encoding rather than
two that agree until they do not"* — and `Json.read(bytes, Class<T>, what)` is all a reader needs. Two things
stand in the way, both small and both worth doing regardless:

1. **The projection records live in the wrong module.** `Projector.Convergence`, `ProjectedFinding`, `Trend`,
   `Tasks`, `TenantSummary` and `Housekeeping.Report` are nested in `supervisor-core` classes that hold a
   store. They are pure data and belong in `supervisor-model` beside `model/status/*`, which already holds
   the tenant-facing projections. Moving them is a rename, and it puts them in the module that owns `Json`.
2. ~~**Nothing has ever read these documents back**, so no round-trip test pins them.~~ **Done, differently.**
   `SupervisorKeysDocumentTests` runs a real pass, extracts the JSON examples from `SUPERVISOR_KEYS.md` and
   compares field *paths* both ways. It does not round-trip through the records — it pins the schema against its
   only specification, which is the part that had rotted. A ninth test asserts every documented section in §1 and
   §4 is either checked or explicitly marked as unwritten, so a section cannot drift back into describing
   something imaginary.

The prediction this section made — that **the convergence document's documented shape and its actual shape have
drifted apart in both directions** — was right in all five rows, and the audit found three more. All resolved:

| field | was | now |
|---|---|---|
| `epoch` | documented, absent | **published.** A split brain is meant to be diagnosable from what the system already wrote, and the assignments carried it while the document an operator reads did not |
| `work_ceiling_hit` | documented, absent (`Plan.ceilingHit` computed and dropped) | **published** |
| `etcd_watch_failures` | documented, absent | **dropped from the spec.** It is the store's counter, not this process's; `watch_poll_failures` is what the supervisor can observe without reaching into a backend |
| per-tenant `passes_since_empty` | documented, published as `oldestPasses` | **spec corrected** to `oldest_passes` |
| `placeable_nodes`, `unreadable_documents`, `tasks`, `balance` | published, undocumented | **documented** |
| `serving` on a shard placement | published, undocumented | **documented and made deliberate** — see below |
| `reachable` on a node status | published, undocumented | **removed** (`@JsonIgnore`); nothing read it and it is derivable from `liveness` |
| `delayed_unassigned_shards` | documented, absent | **dropped from the spec**; it counts OpenSearch's deliberate post-node-left delay, which this service does not model |

**The `serving` row is the one worth reading.** It existed only because `isServing()` is named the way Jackson
detects a property — and the data plane's coordinator routing was already reading it. Deleting it as an
unintended field, which is the correct fix this repository has made twice for that exact pattern, would have
routed queries to nothing in another repository with nothing failing in this one. That is the strongest argument
in this document for the schema test existing: the dangerous direction of drift is not the missing field, it is
the accidental one that somebody has started depending on.

This was exactly what `README.md` says about the three contract documents — *"A contract in prose drifts"* —
happening to the one family of documents that had no reader to keep it honest.

---

## 4. Stage 1 — the rule library and a one-shot command

### 4.1 Why one-shot first

- It runs anywhere a shell does, including from `cron`, including in the demo.
- It is safe to point at production on day one, because it cannot write.
- **Exit status is an interface.** `0` clean, `1` a warning fired, `2` a critical fired, `3` could not read
  the store. That is enough for any monitoring system that can run a command, which is most of them, and it
  needs no agreement with anyone about an alert schema.
- It defers every hard operational question — dedupe, suppression windows, who gets paged, what happens when
  the notifier is down — until someone has an actual on-call rotation to answer them for.

### 4.2 What it reads, and the rule that keeps it a reader

Read-only, and structurally rather than by promise: the rules live in a module that cannot see a store, and
the reader is one small class that is the only thing needing review for write-freedom. Prefixes:

| prefix | what for |
|---|---|
| `supervisor/status/**` | convergence, diffs, oscillation, members, housekeeping — the substance |
| `supervisor/members/**`, `supervisor/pools/*/lease` | who is alive, who holds what |
| `pools/definitions/**` | the denominator: a pool that should be supervised and is not |
| `tenant/*/status/**` | health, awareness — §5.4 |
| `nodes/**`, `tasks/**` | **counts only**, never semantics (§5.3, §9) |

`--store` and backend discovery work exactly as `supervisor-app` already does them, through `ServiceLoader`,
so a missing jar stays distinguishable from a typo.

### 4.3 Output

Two forms, because they have different readers. Default is one line per alert, severity first, carrying the
supervisor's own `because` verbatim — never a paraphrase:

```
CRITICAL POOL_NOT_ADVANCING   shared-a   holder supervisor-a1b2, pass 10432 unchanged for 4m12s
WARNING  ORPHANED_COPY_AGED   acme/uzKx…/1  312 passes: no deletion task names index uuid uzKx…,
                                             so its index may merely be unread
```

`--json` emits the same as an array, for a machine. Nothing else; a formatter is not a feature.

---

## 5. The rules

The important structural observation, which shrinks this section by an order of magnitude:

> **Most alerts are not new predicates. They are age thresholds on findings the supervisor already raises.**

`FindingKind` already carries a `Severity`, and every projected finding already carries `first_observed_pass`
and `passes`. So the bulk of the rule library is **a threshold table, not a detector** — which is what keeps
it from becoming a second differ (§9). Only §5.2 and §5.3 contain genuinely new logic, and they are short
because they are precisely the things a pass cannot see about itself.

### 5.1 Thresholds on existing findings

One row per `FindingKind` that is worth waking someone for, read from `supervisor/status/diff/<tenant>`. The
numbers below are a starting point and the table is the artifact — being able to argue about a number in one
place is most of the value.

| finding | fire at | severity | note |
|---|---|---|---|
| `UNASSIGNED_PRIMARY` | 30 passes | CRITICAL | data is unreachable, not merely un-redundant |
| `UNPLACEABLE` | 30 passes | CRITICAL | `because` names the decider; it is the actionable one |
| `FOREIGN_COPY`, `POOL_REPOSITORY_MISMATCH` | 5 passes | CRITICAL | a configuration error, never a race |
| `NODE_UNREGISTERED` | 2 passes | CRITICAL | see below — never normal, never self-heals |
| `NODE_LOST` | 1 pass | WARNING | the supervisor is already acting; alert only if it persists |
| `UNASSIGNED_REPLICA`, `UNASSIGNED_SEARCH_REPLICA` | 100 passes | WARNING | recovery is slow and legitimate |
| `ORPHANED_COPY` | 60 passes | WARNING | past the 30-pass patience in `DESIGN.md` §22.5 |
| `TASK_STUCK` | 10 passes | WARNING | one per deleted index is the §22.1 signature |
| `STALE_CONFIG`, `PROJECTION_STALE` | 100 passes | WARNING | |
| `UNREADABLE_DOCUMENT`, `UNRECOGNISED_STATE` | 5 passes | WARNING | a version skew or a codec bug |
| `TORN_DESIRED_STATE` | never | — | R2.5 says this is normal. Alerting on it is the classic mistake |

Two rows deserve their reasoning.

**`NODE_UNREGISTERED` is the loudest thing in this table**, out of proportion to its `WARNING` severity in
`FindingKind`. `DESIGN.md` §20.2 explains why: a node with a heartbeat and no registration is *"placeable for
nothing, needing a restart to recover."*

It is also the **regression alarm for `NodeReaper`**, and that is no longer hypothetical. The reaper is safe
because `NODE_CONTRACT.md` now has a node rewrite its registration when it observes the document missing — but
the contract is explicit that *"a node that does not implement this rule is still contract-conformant for
everything else"*, so a wrongly-reaped node running an older plugin strands itself exactly this way. A fleet
mid-upgrade is where that happens, which is a scheduled event rather than an accident, and this row is what
turns it into a page instead of a mystery.

**`SURPLUS_COPY` and `DESIRED_COPY_IGNORED` are deliberately absent.** Both are ordinary mid-convergence
states; the thing worth alerting on is a *shard* alternating between `UNASSIGNED_REPLICA` and `SURPLUS_COPY`,
which is oscillation and already has its own document (§5.2).

### 5.2 The supervisor itself — what a pass cannot see about itself

These are the rules that justify the component. **Six of the eleven are computable from the store today**; the
claim that all of them were did not survive checking the keys, and the gap is marked in the table rather than
discovered during implementation. Every blocked rule is blocked on a document that is *described* in
`SUPERVISOR_KEYS.md` and written by nothing — which is the same defect §3.1 is about, one level further on.

| id | condition | severity | computable |
|---|---|---|---|
| `POOL_UNSUPERVISED` | a key under `pools/definitions/` with no live holder of `supervisor/pools/<p>/lease` | CRITICAL | today |
| `POOL_NOT_ADVANCING` | lease held, but `convergence.pass` unchanged across samples spanning ≫ the pass interval | CRITICAL | today |
| `NO_SUPERVISOR_MEMBERS` | `supervisor/members/` is empty | CRITICAL | today |
| `REGISTRATION_REWRITTEN` | any node reporting `registration.rewrites_after_going_missing` > 0 | CRITICAL | **blocked** — the counter is on the node's `/_clusterless/status` endpoint only, and this reader reads the store |
| `NODE_REAP_BLOCKED` | a `NodeReaper` `kept` entry persisting across samples | WARNING | **blocked** — §7.5; the report is held in a field and never projected |
| `HOUSEKEEPING_ABSENT` | no holder of `supervisor/leases/housekeeping`, or `housekeeping.observedAt` stale | WARNING | today |
| `PASS_SATURATED` | `convergence.passDurationMillis` ≥ 50% of the member document's `passIntervalMillis` | WARNING | **blocked** — `supervisor/status/members/<id>` is written by nothing |
| `WATCH_DEAD` | `watchPollFailures` rising, read against `store_backend` from the member document | WARNING | **partly** — the counter is in `convergence`; the backend name is in the unwritten member document |
| `OSCILLATING` | anything in `supervisor/status/pools/<p>/oscillation` | WARNING | **blocked** — no detector, no document |
| `DIFF_STABLE_NON_EMPTY` | `findingsTotal > 0` and `trend.delta ≥ 0` and `passesSinceEmpty` large | WARNING | today |
| `REGISTRY_IS_FALLBACK` | `housekeeping.registrySource` is not `registry` | INFO | today |

Notes on the ones that are not obvious:

- **`POOL_NOT_ADVANCING` is the alert this whole document exists for.** It is revision-silence liveness
  (R3.3) applied one layer up, to the supervisor instead of to a node, using a mechanism this system already
  trusts. Nothing inside the supervisor can fire it.
- **`DIFF_STABLE_NON_EMPTY` is R7.2 stated as code.** `REQUIREMENTS.md`: *"A diff that is stable and
  non-empty is the alarm."* `Trend`'s own javadoc says *"zero with a non-empty diff is the alarm."* The signal
  has existed since the projections did; nothing has ever evaluated it.
- **`HOUSEKEEPING_ABSENT` is the monitor-of-the-monitor rule.** The housekeeping document is the only source
  for §5.4's entire tier, so if its holder is dead those alerts go quiet rather than firing. Quiet must not be
  the same as clean.
- **`REGISTRY_IS_FALLBACK` is an alert about another alert being blind.** Per `SUPERVISOR_KEYS.md` §6, until
  the control plane writes `tenants/<id>` the tenant list comes from configuration, so `TENANT_UNPOOLED` can
  only ever be as complete as a config file. An operator reading a clean `unpooled_tenants` deserves to know
  which list produced it. This is open question 4 surfacing as an operational fact.

  Its condition was written against `!= "tenants/"`, taken from an example in `SUPERVISOR_KEYS.md` that was also
  wrong: the field is `registry` or `configuration`, never `tenants/`. As written the rule fires on every healthy
  cluster for ever. A worked example of why a rule against a documented value, rather than against the value, is
  not worth the confidence it reads with.

- **Five rules are blocked on documents that do not exist**, and it is worth being precise about the shape of
  that. Each is described in `SUPERVISOR_KEYS.md` in the present tense, so each was written here in good faith,
  and none of them can be evaluated. Two consequences for sequencing: `PASS_SATURATED` and `WATCH_DEAD` want
  `supervisor/status/members/<id>`, which is also R3.3's requirement that the effective liveness threshold be
  observable — so implementing it discharges a requirement and unblocks two rules at once, and it is the cheapest
  item in §7. And `REGISTRATION_REWRITTEN` needs a change in the *data plane*: the rewrite counter is on its HTTP
  endpoint, and a store-only reader cannot see it. Asking for it in the heartbeat is the natural request, since
  "self-healing hides the thing it heals" is the whole reason the counter exists.
- **`WATCH_DEAD` must be read against the backend.** `SUPERVISOR_KEYS.md` §4 notes the poll counter stays
  zero by design on remote backends — so a *non-zero* count on etcd or ZooKeeper does not mean polling is
  failing, it means an assumption about which code path is running is wrong. Both readings are worth an
  alert and they are different alerts.
- **`REGISTRATION_REWRITTEN` is the sharpest rule in this document, and the data plane asked for it.**
  `NODE_CONTRACT.md` now has a node rewrite its registration when it observes the document missing, which is
  what made `NodeReaper` safe — and it says why the counter must be watched: *"Self-healing hides the thing it
  heals: one rewrite is an incident, and a steady trickle is something reaping the registrations of live
  nodes."* A self-healing mechanism converts a visible failure into an invisible one, so the counter is the
  only evidence that the reaper is deleting documents belonging to nodes that are merely unreachable. One is
  worth waking someone for; the alert is CRITICAL at a threshold of one.
- **`NODE_REAP_BLOCKED` is the opposite direction** — the reaper declining, with a reason, pass after pass.
  `NodeReaper.Report.kept` carries the `because`, so this is a threshold on an existing explanation rather
  than a new judgement. It needs §7.5 first.

### 5.3 The leaks — the rules that need a wall clock

The reason the component is worth having beyond a threshold table, and the reason §1.1 is decisive. Each is a
**count sampled over time**, so each needs durable history the supervisor may not keep.

| id | signal | why nothing else sees it |
|---|---|---|
| `NODE_DOCUMENTS_ACCUMULATING` | keys under `nodes/registration/`, `nodes/heartbeat/` or `nodes/shards/` growing while the live node count is flat | the reaper's own failure is silent — see below |
| `ASSIGNMENT_ORPHANED` | `nodes/assignment/<id>` for an id with no registration and no heartbeat | this key is **the supervisor's own**, so an accumulation is a bug here |
| `TASKS_ACCUMULATING` | `tasks/index-management/` count rising with a flat completion rate | `Tasks` gives per-pass rates, never the standing pile |
| `ORPHAN_NEVER_REAPED` | `ORPHANED_COPY` carrying the "no deletion task" refusal, for hours | §22.4 accepts this for ever, on purpose |
| `KEYSPACE_GROWTH` | total keys under each top-level prefix, sampled | crude, and it is what found the bug that prompted this |

**A reaper that has stopped is indistinguishable from a fleet with no corpses in it**, which is §1.2 one level
down and the reason the first row survives `NodeReaper` rather than being obsoleted by it. `NodeReaper` reaps
on an aged `NODE_LOST`, so a backlog is bounded in a healthy system and any *sustained* growth means a rule is
declining, a pool is unsupervised, or the reap is failing its compare-and-set every pass. None of those log
anything an operator reads. Note the pairing with `REGISTRATION_REWRITTEN` in §5.2: this row catches the
reaper doing too little, that one catches it doing too much, and the dangerous direction is the one that heals
itself quietly.

**`ASSIGNMENT_ORPHANED` is the one to build first.** `DESIGN.md` §20.2's original reason for not reaping was
that three of a departed node's four documents are not the supervisor's to delete — but `nodes/assignment/<id>`
**is**, written by it and by nothing else. So it needed no contract change, no ageing and nobody's agreement,
which means an accumulation there is not a trade-off anybody chose. It is a bug on our side, and finding one
is directly actionable.

The remaining rows are deliberately *not* actionable by the alerter, and §9 forbids it from acting. They exist
because §22.4 accepts some orphans for ever on purpose — *"the cost is stated plainly… holds that copy for
ever. Accepted, because only one of the two possible mistakes can be undone."* An accepted cost still needs
somebody to notice when it stops being small, and that sentence is the case for this component in the design
document's own words.

**Storage for samples.** Stage 1 keeps the previous sample in a small file next to the command, which is
enough for `POOL_NOT_ADVANCING` and gives every rate rule a same-order answer. Stage 3 exports these six as
gauges and lets a real time-series database own retention and trend. **Do not build a time-series database
in the KV store**: `SUPERVISOR_KEYS.md` §5 is emphatic that no document may scale with entity count, and a
growth history is the one thing that must scale with time.

### 5.4 Nothing is managing this

Straight from `supervisor/status/housekeeping`, and the reason `Housekeeping.Report` has the fields it has.
Each is a configuration mistake that is otherwise completely silent:

| field | id | severity |
|---|---|---|
| `unpooledTenants` | `TENANT_UNPOOLED` | CRITICAL |
| `poolsWithoutDefinition` | `POOL_WITHOUT_DEFINITION` | WARNING |
| `unownedPools` | `POOL_UNOWNED` | CRITICAL |
| `unpooledNodes` | `NODE_UNPOOLED` | WARNING |
| `duplicateDefaults`, `overloadedDedicatedPools` | as named | WARNING |
| `unreadable` | `UNREADABLE_DOCUMENT` | WARNING |

`unpooledTenants` is the highest-value alert in the document after §5.2, and `SUPERVISOR_KEYS.md` §4 says
exactly why: a tenant the control plane is serving, with indices, that no supervisor manages *"produces no
error anywhere else in the system — no shards are placed, no health is computed, no tasks are reaped, and
every one of those looks like idleness."*

### 5.5 The cluster is unhealthy — deliberately last

Red indices from `tenant/<t>/status/cluster`. This tier is last on purpose: it is the only part of this document
an operator's existing OpenSearch monitoring already covers, and duplicating it is how an alerter earns its
reputation for noise.

**`status/awareness` is not a source of violations**, which is worth correcting rather than leaving as a
plausible-sounding line. It carries the *values that exist* for the pool's declared awareness attributes — it says
`zone: [z1, z2]`, not that anything is wrong. Two rules are actually available, and they are different from each
other:

- **A declared attribute whose value set has shrunk.** `SUPERVISOR_KEYS.md` names the hazard: a zone whose nodes
  are all down silently stops being a valid weighted-routing target, so a request naming it is rejected for a
  reason that reads like a typo. That needs the *previous* sample, which is exactly what this component has and
  the supervisor does not.
- **An unsatisfiable awareness requirement**, which is already a finding — `UNPLACEABLE`, with the allocator's
  sentence in `unplaceable_because` — and belongs in §5.1 with the rest of them rather than here.

Note that the document narrowed while this was being written: it now offers **only the pool's declared
awareness attributes**, where it used to publish every attribute a node carried. A rule counting its keys would
have been counting `remote_store.segment.repository` among the routing dimensions.

---

## 6. The alarm is on age, never on presence

The law is already written in this repository, twice, and it is the single rule most likely to be violated by
a well-meaning addition. `SUPERVISOR_KEYS.md` §4: *"The alarm is on **age**, never on presence."*
`FindingAges`: *"R2.5 promises the supervisor will observe an index without its alias, so the alarm has to be
that a finding is **old** rather than that it exists."*

Every row in §5.1 therefore has a threshold, `TORN_DESIRED_STATE` has none by design, and a rule that fires
on the mere existence of a finding does not pass review. The cost of getting this wrong is not a false page;
it is the alerter being muted in week one, after which it is worse than nothing because its silence now means
two different things.

One corollary for the thresholds in passes: they are only comparable within a process until §7 adds a wall
clock. Until then, prefer the alerter's own observation time for anything spanning hours.

---

## 7. Stage 2 — four things the projections do not yet say

Supervisor-side work, each small, each independently useful, and none of it blocked on stage 1 or 3.

1. **A wall-clock `firstObservedAt` beside `firstObservedPass`.** Passes do not survive a restart and do not
   compare across supervisors, which makes every age-based rule weaker than it looks. One field, and R2.3 is
   untouched because nothing acts on it.
2. **`registrationsTotal` and `registrationsSuperseded` per pool.** §20.1 already computes this when it picks
   a winner per `address:port` and then discards it. Publishing the count un-hides the leak without undoing
   the fix — the projection still shows one node per endpoint.
3. **A count of refusals that have never resolved, by reason code.** `DESIGN.md` §22.4 confesses the current
   mechanism outright: *"an unexplained finding sitting for ever is how the previous version of this was
   eventually noticed — by someone reading a status document in confusion."* `Decision`'s reason slug already
   exists (§16) and is the natural grouping key.
4. **The fields §3.1 found missing** — `epoch`, `work_ceiling_hit` (`Plan.ceilingHit` is computed and thrown
   away), `etcd_watch_failures`. `work_ceiling_hit` standing true for many consecutive passes is the
   difference between a burst and a pool permanently behind, and R2.6 is the reason the flag exists.
5. **Project `NodeReaper.Report`.** Today it is returned from `SupervisorLoop.lastReaping()` and held in a
   field, so the `because` behind every document the supervisor *deleted*, and every one it declined to
   delete, exists only in the memory of the process that decided it. That is §1.2 applied to the most
   destructive thing this service does: a reap cannot be reviewed after the fact, and `NODE_REAP_BLOCKED`
   cannot be evaluated at all. A bounded document under `supervisor/status/pools/<p>/reaping`, capped like
   every other status document, with the `kept` list carrying its reasons.

---

## 8. Stage 3 — the long-running component, if it is ever needed

Only when someone has a rotation to page. Then: a single instance, leader-elected on its own key through the
existing `LeaseManager`, exactly as housekeeping is. Its own `monitor/` prefix for dedupe and suppression
state, and nothing else in the store.

Split its output two ways, because the two families want different machinery:

- **Numeric and trend signals (§5.3) become gauges** in whatever metrics system the operator runs. Retention,
  trend and routing are solved problems; do not solve them again.
- **Semantic signals (§5.1, §5.2, §5.4) are evaluated in-process**, so the notification body can carry the
  supervisor's `because` verbatim. That text is the entire reason these alerts are actionable, and it does not
  survive being reduced to a gauge and reconstituted by a query language.

It is not a fourth plane, and the test for that is concrete: **it writes nothing any other component reads.**
It is an operator tool that happens to run continuously, beside the architecture rather than in it. The
three-plane model in `REQUIREMENTS.md` §1 stays true as written.

---

## 9. Two guardrails, to be enforced rather than intended

**It may count keys and read timestamps. It may never form an opinion the supervisor could have formed.**

If a rule needs a judgement that cannot be made from a projection, the fix is a new field in a projection,
not new logic here. This is the rule that keeps the alerter from becoming a shadow differ with a different
opinion, and `REQUIREMENTS.md` §8 gives the precedent: two implementations of one semantics is how components
silently disagree. The module graph enforces most of it — rules that cannot see a store cannot invent state —
and §5.3's raw-prefix access is confined to counting, which is why it is spelled out as counting.

**It has no authority. It never writes a key another component owns, and it never acts.**

`DESIGN.md` §20.2 is the specific hazard: deleting the registration of a node that is merely partitioned
strands it as `NODE_UNREGISTERED`, *"needing a restart to recover"*, and §20.2 chose a slow leak over that
outcome deliberately. An alerter that grew a cleanup mode would relitigate that decision by accident, in
production, at 3am. Noticing and acting are separated here on purpose, and the separation is the design.

---

## 10. Testing

The same split that works everywhere else in this repository, per `DESIGN.md` §10 and `WORKING_NOTES.md`:
risk goes where no network is needed.

- **Rules are pure functions of hand-built snapshots**, so every threshold and every tier gets a unit test
  with no store at all. This is the bulk of it and it is cheap.
- **Round-trip tests on every projection record** — write with `Projector`, read with `Json.read`, assert
  equality. That is what §3.1's drift table costs to prevent, and `KeysTests` is the precedent for pinning a
  contract with a test rather than with prose.
- **One integration test per backend** in `supervisor-integ`, following the existing abstract-suite pattern:
  run a pool to a known-bad state, assert the expected alert fires and that nothing else does. The second
  half is the one that catches noise.
- **A negative test that matters more than it looks:** a supervisor killed mid-run must produce
  `POOL_NOT_ADVANCING` and must produce it from the store alone, with no supervisor process alive. If that
  test can be written without a running supervisor, §1.2 is satisfied by construction.

---

## 11. What this does not do

- **No acting, ever.** §9.
- **No history beyond what §5.3 samples.** A real trend needs stage 3 and a metrics system.
- **No per-alert routing, escalation or schedules.** That is the notifier's, and in stage 1 it is `cron`'s.
- **No opinion on data-plane internals.** Anything needing a node RPC is out by R3.4.
- **Nothing about the control plane's own health.** It is a separate service with its own lifecycle; the only
  thing observable from here is the shape of what it wrote, and `REQUIREMENTS.md` §1 keeps it that way.
- **The thresholds in §5.1 are unvalidated.** They are first guesses from reading the design documents, not
  measurements. Expect every one of them to move, and treat the table's existence as the deliverable.

---

## 12. Demo mode is not a compromise here

The bugs that prompted this document were found on a laptop, with a long-lived filesystem store, services
starting and stopping constantly, and indices repeatedly created and deleted under the same names. It is
tempting to discount that environment as unrealistic. It is not — it is **fast-forwarded**.

Index-name reuse happens in production: tenant re-onboarding, restored snapshots, CI namespaces, any
create-delete-recreate automation. And `DESIGN.md` §22.1 symptom 3 is what it costs — one index deleted and
recreated under the same name *"silently disables pointer resets on that node, permanently."* A monitor would
never have caught that; it is a correctness bug and it was found by running the thing. The leaks are the part
alerting addresses, and they are real on both timescales: a laptop reaches the interesting state in an
afternoon, and production reaches it in a quarter.

Which is the argument for building stage 1 now rather than after the feature work. Two properties the
supervisor is entitled to be proud of — a pure reconciliation core and a refusal to hold memory — are exactly
what make it unable to notice slow decay on its own. **A system that can explain every refusal it made and
cannot tell you that it stopped making decisions is not yet production-ready, and the gap is one command.**
