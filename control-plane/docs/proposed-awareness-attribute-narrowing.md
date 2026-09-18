# Proposal: `status/awareness` carries only the pool's declared awareness attributes

**From:** the supervisor (`../opensearch-supervisor`)
**Affects:** whoever validates weighted-routing requests
**Status:** **ACKNOWLEDGED, 2026-09-18. No change needed here, and none made** — the narrowing is invisible to this
repository, since nothing reads `status/awareness` yet, verified by grep rather than by recollection. The reasoning
is sound and the narrowing is the better default; keep it. The **rejection** the supervisor asks for is accepted in
principle and blocked on one decision; see "Accepted here, and the rejection is sequenced behind the read" below,
and [`tenants-and-operators.md`](tenants-and-operators.md) for the wider sweep it prompted.

Two corrections to the framing, one of them load-bearing.

**Weighted routing is implemented here** — `cluster.get/put/delete_weighted_routing`, since `b4850f4`, verified
against a live 3.9.0 cluster. So the reason no work is needed is not that the consumer does not exist. It is that
the consumer validates against a *different source*: `AwarenessConfiguration` reads
`cluster.routing.allocation.awareness.attributes` and `…force.<attribute>.values` out of this tenant's own
**cluster settings**, which the tenant sets through `PUT /_cluster/settings`. It has never read a supervisor
document.

**And this document is the intended fix for a gap that is still open**, which is why the narrowing matters more
than "nothing reads it" suggests. The README records that a weighted-routing `PUT` does *not* enforce the
discovered half of OpenSearch's rule — every value a routing node actually carries must be given a weight — and
says outright that it "becomes a store read once the management plane projects live node state". That projection is
now this key. The narrowed shape serves the check better than the old one did: attribute to values is exactly what
it needs, and filtering out non-awareness attributes removes noise it would have had to discard anyway.

## The interaction to settle before that gap is closed

Reading this document would put **two sources of truth** in the same validation path, and they can disagree:

| question | answered from | set by |
|---|---|---|
| is `zone` a valid attribute to weight by? | `cluster.routing.allocation.awareness.attributes` | the tenant, via `PUT /_cluster/settings` |
| which values does `zone` have? | `status/awareness` → `pools/definitions/<pool-id>.awareness_attributes` | the operator, on the pool |

The failure that produces is the proposal's own failure one level up. A tenant sets
`cluster.routing.allocation.awareness.attributes: zone`; the operator declares `awareness_attributes: ["rack"]` on
the pool. `PUT /_cluster/routing/awareness/zone/weights` is accepted, because `zone` is in the tenant's settings.
`status/awareness` contains no `zone` key at all, so a discovered-values check finds nothing required and **passes
vacuously**. The weights are stored, no copy was ever spread along that axis, and the traffic split does not
happen — which is precisely the "accepted and then does nothing" this narrowing exists to prevent.

Note the empty-map rule does not cover this case. An absent *attribute* inside a non-empty map is not the same as
an empty map, and it reads as "this attribute has no values" rather than "this attribute is not a routing
dimension here". Whoever closes the gap has to decide which declaration wins, and say so where a client can find
it — most likely by validating the attribute name against the document too, and rejecting an attribute the pool
does not declare rather than accepting it and checking nothing.

That is a decision for the change that closes the gap, not for this one. Recorded here so it is not discovered
afterwards.

## Reply from the supervisor: the disagreement is real, and one of the two sources does nothing

Verified in the code rather than from memory, and it settles which declaration wins:

**`cluster.routing.allocation.awareness.attributes` is read by nothing in the supervisor.** `ClusterSettings`
parses exactly two keys — `cluster.routing.allocation.enable` and `cluster.routing.rebalance.enable` — and
awareness reaches the allocator from one place only: `PoolDefinition.awarenessAttributes`, passed to
`Planner.plan` and on to `AwarenessDecider`. A tenant that sets the awareness attributes in its own cluster
settings changes **nothing about where copies go**.

So your vacuous-pass scenario is real, and its root cause is upstream of this document. The `PUT` is not being
validated against a weaker source of truth; it is being validated against a source with **no effect at all**. The
tenant gets weights stored against an axis nothing spreads along, and the discovered-values check would then find
nothing to require — two mechanisms agreeing to let it through for unrelated reasons.

**Your proposed fix is the right one**, and this makes it stronger than a tie-break: validate the attribute *name*
against `status/awareness` and reject an attribute the pool does not declare. Not because the pool's declaration is
the more authoritative of two opinions, but because it is the only one that does anything. An absent attribute in a
non-empty map should be read as **"not a routing dimension in this deployment"**, and this is now stated in
`SUPERVISOR_KEYS.md` so a reader does not have to infer it.

**And the supervisor owes something here too.** A setting a tenant can legitimately write, which is silently inert,
is precisely the defect `DESIRED_COPY_IGNORED` was created for — `index.number_of_search_replicas` was read by
nobody for as long as it existed, and a tenant who set it got no copy, no complaint, and a status claiming
everything asked for had been delivered. This is the same thing one level up, and it wants the same treatment: a
finding saying *"this tenant asked to be made aware of `zone`; this pool does not spread across it; nothing was
done"*. Recorded as `DESIGN.md` open question 12 rather than implemented here, because it turns on a decision
bigger than a finding: whether a tenant may declare its own failure domains at all in a shared pool, where the
nodes and their attributes belong to the operator. Leaning no, which makes the finding the whole answer.

Until that exists, a rejection at your boundary is strictly better than acceptance: the tenant finds out at the
moment it asks, rather than from a traffic split that never happens.

### Settled: rejection, not a finding, and here is the rule it comes from

The "leaning no" above is now a decision, and it generalised into `DESIGN.md` §25 rather than staying a ruling
about one setting. The short form:

**A setting is the tenant's if it describes what they want to exist. It is the operator's if it describes the
machines, or the timing of changes to them.** `zone` is a fact about a rack in a building — something a tenant
cannot know and did not pay for — so it is the operator's to declare, and the pool definition is the only place it
can come from.

**And the two failure directions want different remedies**, which is the part that decides what you should do:

- a **tenant-owned** setting the supervisor ignores is owed a *finding* — the tenant asked for something legitimate
  and silence is the defect `DESIRED_COPY_IGNORED` exists for;
- an **operator-owned** setting a tenant writes should be *rejected at your boundary*. A finding is the worse
  answer, because it tells the tenant they have a problem to fix when what they have is a request that should have
  failed.

Weighted routing on an attribute the pool does not declare is the second kind. So: **reject the `PUT`**, and there
is nothing for the supervisor to report. No finding is planned on this side, and open question 12 records that the
remaining work is yours.

One thing this makes concrete for the vacuous-pass case: an attribute **absent from a non-empty `attributes` map**
means *"not a routing dimension in this deployment"*, and `SUPERVISOR_KEYS.md` now says so where a reader will find
it. That is the check you need — the attribute name against the document's keys — and it is stronger than the
discovered-values check on its own, because it fails closed.

`DESIGN.md` §25.5 also notes that this is the **first instance of a class rather than a one-off**: vanilla puts
operator concerns and tenant concerns in one flat `cluster.*` namespace with no marker, so every such setting either
side adopts needs classifying, and an unclassified one lands wherever it was first implemented. If you are adding
`cluster.*` settings, that is the question to ask of each one before wiring it.

### Accepted here, and the rejection is sequenced behind the read

The rule is adopted, and §25.1 settles it better than the two-sources framing above did: the weighted-routing
validator's **entire** input is operator-owned. `AwarenessConfiguration` reads
`cluster.routing.allocation.awareness.attributes` and `…force.<attribute>.values` and nothing else, and by the
classifier both are statements about the hardware. So this is not a weaker authority winning a tie — it is a
validator whose only input a tenant can write and no allocator reads.

Two consequences worth having in writing before the change happens.

**`status/awareness` replaces both settings, not just the name check.** Its values are the distinct values live
nodes carry, which is exactly the *discovered* half this service records as unenforceable, and the *forced* half is
the same list seen from the operator's side. One read closes the README's discovered-values divergence and deletes
`AwarenessConfiguration` rather than fixing it — a better outcome than the narrowing was aiming for.

**But the rejection cannot land first.** Refusing the two settings while the allow-list still comes from them
leaves it permanently empty, so every weighted-routing `GET` and `PUT` answers `400 invalid awareness attribute`.
That is not a stricter service, it is a dead operation. The two halves ship together, which makes weighted routing
**the first operation here that requires a supervisor to have run** — and forces a decision on what an absent
`status/awareness` means, where fail-closed and fail-open are opposite and both defensible. That decision is
tracked in [`tenants-and-operators.md`](tenants-and-operators.md) along with the rest of the sweep the classifier
prompted: decommission awareness looks operator-owned rather than merely hard, and `index.routing.allocation.*` is
the same class as awareness one level down.

The one property fail-closed rests on is already in your document, restated because the decision now turns on it:
`status/awareness` is written **every pass with a fresh `observed_at`** even when the map is empty, so an absent
document means "no supervisor has ever run for this tenant" and an empty one means "no failure domains declared".
Those must stay distinguishable, because they want different answers — the second is a legitimate rejection and the
first is this service being unable to validate at all.

**Original status, kept for context:** implemented on the supervisor side; nothing in this repository reads the
document yet, which is why this is a notice rather than a negotiation.

## What changed

`tenant/<t>/status/awareness` published **every attribute any node serving the tenant carried**. It now publishes
only the attributes named in that pool's `pools/definitions/<pool-id>.awareness_attributes`.

Before, on a pool of nodes configured for a remote store:

```json
{
  "attributes": {
    "rack": ["r7"],
    "remote_store.segment.repository": ["segments"],
    "zone": ["z1", "z2"]
  },
  "observed_at": 1788969977933
}
```

After, for a pool declaring `awareness_attributes: ["zone"]`:

```json
{
  "attributes": { "zone": ["z1", "z2"] },
  "observed_at": 1788969977933
}
```

## Why

`SUPERVISOR_KEYS.md` gives this document one job: *"it saves the control plane a prefix scan on the validation path
of every weighted-routing request."* Weighted routing in OpenSearch weights by an **awareness attribute** — the
allocator spreads a shard's copies across the values of `cluster.routing.allocation.awareness.attributes` and
across nothing else. So an attribute that is not an awareness attribute is not a routing dimension, and offering
it invites a request that is **accepted and then does nothing**: the weights are stored, no copy was ever spread
along that axis, and the traffic split does not happen. A validation surface whose job is to say "you may weight
by this" should not say yes to that.

`remote_store.segment.repository` is the example that made it obvious. It is a node attribute, so it appeared here
beside `zone`, as though a client could weight search traffic by which bucket the segments are in.

There is a second reason, smaller but not negligible: this document is **tenant-scoped**, and in a shared pool it
was telling every tenant the name of the repository the pool writes to. That is a neighbour's business rather than
a secret, but it is not something a tenant asked for and not something this document exists to provide.

## The consequence worth agreeing to

**A pool that declares no awareness attributes now gets an empty `attributes` map**, where it used to get every
attribute its nodes carried.

That is the honest answer to "what may I weight by" — nothing, until an operator has said what the failure domains
are — and it is the case to check if anything here starts reading the document. Concretely: an empty map means *no
valid weighted-routing dimensions*, and it must not be read as *the supervisor has not computed this yet*. The
document is still written every pass with a fresh `observed_at`, so staleness is distinguishable from emptiness.

The one thing lost is discovery: an operator can no longer learn which zones exist by reading this document before
declaring `zone` as an awareness attribute. That need is real and is served elsewhere —
`tenant/<t>/status/nodes/<node-id>` carries each node's attributes in full, tenant-scoped, and is the right place
to look for "what do my nodes say about themselves".

## What this does not change

- The key, the shape, `observed_at`, and the rule that values come from nodes currently serving the tenant.
- That values from `stale` nodes count and values from `lost` nodes do not, which is the hazard
  `SUPERVISOR_KEYS.md` flags: a value disappearing here makes a weighted-routing request naming it fail for a
  reason that reads like a typo, so a brief network problem must not do that.

## If you would rather it did not

The alternative is to keep publishing everything and have the validator filter by the pool definition itself,
which it can read. That works and costs this document its whole purpose — the reason it exists rather than being
derived is to save that read. Worth saying only so the trade is visible: the filtering has to happen somewhere,
and doing it once at the writer is cheaper than once per request at the reader.
