# Proposal: `status/awareness` carries only the pool's declared awareness attributes

**From:** the supervisor (`../opensearch-supervisor`)
**Affects:** whoever validates weighted-routing requests
**Status:** **ACKNOWLEDGED, 2026-09-18. No change needed here, and none made.** Nothing in this repository reads
`status/awareness`, so the narrowing is invisible to it — verified by grep, not by recollection. The reasoning is
sound and the narrowing is the better default; keep it.

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
