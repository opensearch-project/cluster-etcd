# Tenants and operators: the second axis of scope

**Where the rule comes from:** `../opensearch-supervisor/DESIGN.md` §25, added in `f14f3d4`. The classifier is one
sentence:

> A setting is the **tenant's** if it describes what they want to exist. It is the **operator's** if it describes
> the machines, or the timing of changes to them.

Vanilla OpenSearch does not need this rule, because it assumes the party that runs a cluster and the party that
uses it are the same person: one `cluster.*` namespace, one admin role, and nothing in the model separating "I want
this index" from "I own these machines". Here they are different parties.

## Why this repository needs a second axis

[The README's line](../README.md#short-lived-crud-only-is-the-line-and-it-decides-scope) — short-lived CRUD,
answerable from metadata alone — asks *can this service serve the request?* Ownership asks *may this caller make
it?* The two are independent, and the gap between them is real: **an operation can be pure metadata, finish inside
its own request, and still not belong here**, because the thing it configures is a fact about hardware the tenant
neither knows nor paid for.

`spec/control-plane-scope.yaml` decides scope on the first axis only, which is why the items below were classified
by how hard they were rather than by whose they are. That is not an oversight so much as the predicted outcome:
§25.5 notes that `cluster.*` is one flat namespace with two owners and no marker, so **an unclassified setting
lands wherever it was first implemented.**

## The sweep

The user's read is right: almost the whole surface is tenant-owned. Indices, aliases, mappings, settings that
describe the index rather than the machines, templates, data streams, ingest and search pipelines, stored scripts,
and the pause/resume of a tenant's own ingestion are all *things the tenant wants to exist*. `cluster.health`,
`cluster.state` and `cluster.pending_tasks` are observations, which the tenant is entitled to about its own
indices.

Six exceptions, most-actionable first.

### 1. The weighted-routing validator's entire input is operator-owned

Not one source of two disagreeing — the whole input is in the wrong hands. `AwarenessConfiguration` reads exactly
two settings, and by §25.1 both are the operator's:

| setting | what it states | whose |
|---|---|---|
| `cluster.routing.allocation.awareness.attributes` | which node attributes are failure domains | operator — `zone` is a fact about a rack in a building |
| `cluster.routing.allocation.awareness.force.<attribute>.values` | which values must exist across the fleet | operator — the same fact, enumerated |

Both are read from `cluster/settings`, which only the tenant can write. And the supervisor has verified the other
half: `ClusterSettings` there parses just `cluster.routing.allocation.{enable,rebalance.enable}`, and awareness
reaches the allocator solely from `PoolDefinition.awarenessAttributes`. So a tenant setting these **changes nothing
about where copies go**. It is not a weaker authority; it has no effect at all.

The fix is the one the supervisor asks for: validate the attribute name against `tenant/<t>/status/awareness` and
reject an attribute the pool does not declare, reading an attribute absent from a non-empty `attributes` map as
*"not a routing dimension in this deployment"*. Two things make it more than a tie-break:

- **`status/awareness` replaces both settings, not just the name check.** Its values are the distinct values live
  nodes carry, which is precisely the *discovered* half this service records as unenforceable — and the forced half
  is the same list from the operator's side. One store read closes the README's discovered-values divergence and
  removes `AwarenessConfiguration` rather than fixing it.
- **The remedy has to be a rejection, not a finding.** §25.3's asymmetry: a tenant-owned setting the supervisor
  ignores is owed a finding, but an operator-owned setting a tenant writes should fail at this boundary. A finding
  tells the tenant they have a problem to fix when what they have is a request that should have failed. No finding
  is planned on the supervisor side; open question 12 records the rejection as ours.

**The sequencing constraint, which is why this is not a one-line change.** Rejecting the two settings without
reading `status/awareness` first leaves `AwarenessConfiguration.attributes` permanently empty, and every weighted
routing `GET` and `PUT` answers `400 invalid awareness attribute` — the operation is dead, not stricter. The two
halves have to land together.

And landing them together decides something this service has not had to decide before: **weighted routing would
become the first operation that requires a supervisor to have run.** With no `status/awareness` document the honest
answers are opposite — fail closed (reject every attribute, which is what an absent document literally implies) or
fail open (accept any, which is today's behaviour minus the check). Worth deciding deliberately, because it is the
first read of a supervisor-written key on a request path and it sets the precedent for the other four projections.

### 2. Decommission awareness is the operator's, not merely hard

`cluster.{get,put,delete}_decommission_awareness` are in `included_operations` and stubbed. The recorded reason is
difficulty — [`where-the-hard-parts-are.md`](where-the-hard-parts-are.md) calls `put` "Tier 4 work in a Tier 0
costume" because it initiates a node drain. Ownership makes it simpler than that: draining nodes is *the machines
and the timing of changes to them*, so it is not this service's to offer at any tier.

The codebase half-knows this already. `WeightedRoutingHandlers` skips the decommission checks "for a scope reason
rather than a data one: decommissioning is management-plane work and no decommission can be in progress here" —
while the scope file still claims the three operations are ours to implement. **Recommend moving them to
`excluded_operations`**, which removes their routes; a client then gets "no such API here" instead of a 501
promising an implementation that should never arrive.

### 3. Weighted routing itself is worth asking the question about

Unresolved, and raised because the classifier points at it. If `zone` is the operator's because a tenant cannot
know it, then *weighting search traffic across zones* is a statement about the same machines — and the reason
anyone reaches for it in vanilla is "zone z1 is degraded, drain it", which is operator intent with operator
timing. A tenant does not know z1 is degraded.

It is also §25.5's complaint about `allocation.enable` one level up: the weights live in a per-tenant document, so
an operator who wants to shift traffic out of a zone must ask **every tenant bound to the pool** to write its own
weights. The pool-wide equivalent would be a pool definition field.

Not proposing a change. The supervisor's proposal assumes this is a control-plane API and only the dimension needs
validating, which is a coherent position — weights are per-tenant policy over a topology the operator declares.
Recorded so it is a decision rather than an inheritance from vanilla.

### 4. Index-level node filters are the same class as awareness

`index.routing.allocation.{require,include,exclude}.<attribute>` pins an index to nodes carrying an attribute
value — a tenant naming a machine fact, exactly the shape §25.1 settles against. Accepted here today: the settings
registry answers whether a setting *exists* and whether it is dynamic, which is the wrong question for ownership,
and `index.routing.allocation.*` is one of the prefix groups it deliberately accepts wholesale.

Unlike awareness this one has no store read waiting for it, so the options are to reject the group or to leave it
as a known instance of the class. Left alone for now; listed because the next `cluster.*` or `index.*` setting
added should be classified before it is wired, not after.

### 5. Snapshot repository registration sits in the intersection

Already in scope, and the short-lived-CRUD test still puts it here: a registration is one key. Ownership qualifies
rather than overturns that. A `fs` repository's `location` must be inside `path.repo` **on every node**, and an
`s3` one names a bucket plus credentials the operator provisions — so the document a tenant writes is partly a
claim about the machines, and a wrong one fails at snapshot time rather than at registration.

That is §25.2's third category (tenant intent, operator capability, answer is the overlap), and it resolves the way
the other two instances did: honour the request up to the capability and report the shortfall. Nothing to do until
the supervisor publishes what the pool provides.

### 6. A `file`-type ingestion source names a path on the data node

`ingestion_source.param.base_directory` for `"type": "file"` is a filesystem path on whichever node polls it — a
machine fact, written by the tenant, and the one place this repo demonstrably already allows it (the end-to-end run
that verified pause/resume used `/Users/.../clusterless-ingestion`). `type: kafka` is different: the tenant's
stream is the tenant's, and bootstrap servers describe their system rather than ours.

So the ownership question here is per source type, not per setting, which no registry keyed by setting name can
express. Recorded rather than acted on: the control plane does not interpret `ingestion_source` at all today, and
the party that must reject an unreachable directory is the one that can see the filesystem.

## What this does not change

Everything else. To be explicit, because "a second axis of scope" invites re-litigating settled calls: indices,
aliases, mappings, data streams, templates, both pipeline kinds, stored scripts, cluster settings that describe the
tenant's own indices, ingestion pause/resume, and every read operation stay exactly as they are. `allocation.enable`
and `rebalance.enable` stay tenant-scoped too — the supervisor re-examined them against §25.1 and kept them, since
the effect is confined to that tenant's indices; what is missing is an operator-scoped equivalent, and that is a
pool definition field rather than anything here.

## The decisions this leaves open

1. **Does weighted routing require a supervisor?** Needed before the awareness rejection can land, because the
   rejection and the `status/awareness` read must ship together, and an absent document has to mean something.
2. **Do the three decommission operations leave the route table?** Mechanical once decided: three lines move in the
   scope file, and the route and group counts in the README change with them.
3. **Is weighted routing a tenant API at all?** §3 above. Answering "yes, with the dimension validated" is a
   position, and the one the supervisor has assumed.
