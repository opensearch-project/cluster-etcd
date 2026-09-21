# Tenants and operators: the second axis of scope

**Where the rule comes from:** `../opensearch-supervisor/DESIGN.md` §25, added in `f14f3d4`. The classifier is one
sentence:

> A setting is the **tenant's** if it describes what they want to exist. It is the **operator's** if it describes
> the machines, or the timing of changes to them.

Vanilla OpenSearch does not need this rule, because it assumes the party that runs a cluster and the party that
uses it are the same person: one `cluster.*` namespace, one admin role, and nothing in the model separating "I want
this index" from "I own these machines". Here they are different parties.

**One companion clause, which the sweep below forced** — the classifier settles settings and does not settle
resources that connect to something outside the cluster:

> A resource naming a **neighbouring service or cloud infrastructure** may belong to either party, and which one is
> a property of the individual registration rather than of the resource type. So there must exist a mechanism for an
> operator to register one that tenants **cannot modify**, and in some deployments **cannot see**.

Not an absolute rule and deliberately not one: the same deployment can have an operator's backup bucket and a
tenant's own bucket registered through the same API. §5 is the whole of it.

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

Six exceptions, most-actionable first. The last is not an exception so much as the reason the others are
categories rather than individual calls.

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

### 5. External infrastructure: ownership is per registration, not per resource type

Snapshot repositories and ingestion sources both name a system outside the cluster, and the classifier does not
settle them — because for these, **ownership is a property of the individual registration rather than of the kind of
thing being registered.** The same OpenSearch concept has operator instances and tenant instances in one deployment:

| case | registered by | tenant may |
|---|---|---|
| Amazon OpenSearch Service's automated snapshots, to a bucket the service team owns | operator | see it, restore from it; **not** modify or remove it |
| a customer's own S3 repository, pointing at their own bucket | tenant | everything |
| Kafka brokers and topics owned by the search team that ran the fleet (a deployment the author worked on) | operator | consume the topic provisioned for them |
| an object-store ingestion source over a tenant's own S3 data, which is being built now | tenant | everything |

So "who owns snapshot repositories?" is not a question with an answer. "Who registered *this* repository?" is. Two
things follow, and the second is the load-bearing one:

- The classifier needs a companion clause rather than an extension. Something like: **a resource that connects to a
  neighbouring service or cloud infrastructure may belong to either party, and there must exist a mechanism for an
  operator to register one that tenants cannot modify — and, in some deployments, cannot see.**
- The control plane therefore has to hold resources **it did not write and cannot change**, and tell them apart
  from the tenant's own. It has exactly one mode today: the tenant wrote it, so the tenant may change it.

The two instances I flagged earlier are the least interesting ones. An `fs` repository and a `file`-type ingestion
source are test-only fixtures — never production — so the fact that both let a tenant name a host path is not a
problem to solve, and the pause/resume verification run leaning on `base_directory` was exactly the intended use.
What matters is the shape they share with the production cases above.

#### Three access levels, and the third costs more than it looks

1. **Tenant-owned** — full CRUD. Today's only mode.
2. **Operator-owned, visible** — present in reads; writes and deletes rejected. This is the automated-snapshot
   shape, and it should be the default: a tenant that can list and restore from the operator's repository can use
   it, which is the point of it existing.
3. **Operator-owned, hidden** — absent from reads entirely.

Level 3 is where it gets expensive, because **invisibility and name collision are in direct tension.** If the
operator has a hidden repository `backups` and a tenant sends `PUT /_snapshot/backups`, every answer is bad:

- *accept* → two resources under one name, and whichever loses resolution is silently unreachable;
- *reject* → the rejection discloses the existence of the thing that was meant to be invisible, and the message has
  to either say why or lie;
- *accept and shadow* → the tenant's registration works and the operator's stops being used, so a guarantee the
  operator made (backups keep running) breaks on a tenant write.

The standard escape is **reserved names**, and it is what Amazon OpenSearch Service does: the automated repository
sits at a reserved name (`cs-automated`, plus an encrypted variant) that a customer's registration cannot take. Then
collision is impossible by construction, and the rejection leaks nothing beyond "that prefix is not yours". Note it
is *visible* there rather than hidden, which is the further evidence that level 2 is the useful one.

**Recommendation: a reserved prefix, visible and read-only by default, with hidden as an opt-in flag permitted only
inside that prefix.** Without reserved naming, level 3 is not merely costly, it is incoherent.

#### Where operator registrations would live

Not in `tenant/<t>/snapshot/repositories/<name>`. That key's single writer is the control plane, and one writer per
key is the invariant that keeps the store safe without cross-key transactions — `KEY_LAYOUT.md` is explicit that
every key has exactly one writer at any moment, and §5.5 of the supervisor's requirements forbids the traffic in
the other direction for the same reason.

The shape that fits what already exists: **the operator declares on the pool, the supervisor projects per tenant,
the control plane reads the projection.** That is `status/awareness` exactly — and the argument that justifies the
projection existing rather than being derived is the same one, that it saves a prefix scan on a request path. It
also matches how these resources actually appear: one broker set or one backup bucket per fleet, not one per
tenant, so a pool-level declaration projected into `tenant/<t>/status/…` is one write per pool and one read per
request.

Three consequences for this service, none of them large on their own:

- **`GET /_snapshot/_all` merges the projection with our own keys**, and cannot tag the difference — the response
  shape is OpenSearch's and has no owner field. AWS does not tag it either; `cs-automated` simply looks like a
  repository. So a tenant learns ownership by being refused, which is acceptable and worth stating deliberately
  rather than discovering.
- **A refused delete needs a status.** `403` implies an authorisation system this service does not have, so the
  400 illegal-argument shape naming the reserved prefix is the honest answer.
- **An absent projection means "no operator resources", and that is fail-open on purpose** — the opposite of the
  awareness decision in §1, and for a good reason. A resource the operator has not declared does not exist, so
  reading nothing permits nothing that should have been forbidden. An *attribute* the operator has not declared is
  different: reading nothing there would permit weights on any axis at all.

#### And the boundary cannot currently tell who is calling

§25.3 says an operator-owned request should be rejected at the control plane's boundary. That presumes the boundary
can distinguish an operator from a tenant, and **ours cannot — every request is a tenant request.** The tenant
resolver produces a tenant id and there is no privileged caller identity.

Which is fine for the reserved-prefix design, because the operator writes its registrations somewhere else
entirely and never calls this API. But it does mean the only rule we can enforce is "*nobody* may write these
through here", and a deployment where an operator genuinely wants to use the same API needs either a privileged
caller or a separate admin surface. Neither exists; recorded so it is a known gap rather than a surprise.

### 6. How much of any of this matters depends on whether tenants reach this service at all

In the Kafka deployment above, tenants **could not create indices directly.** They worked through an
operator-owned provisioning UI, which created the topic and the index together. Every request arriving at the
control plane was therefore an operator's request, and the tenant/operator distinction on this surface was close to
irrelevant — not because the two parties were the same, but because only one of them was a caller.

That is the variable to keep in mind whenever this document says "reject at the boundary". Three things follow:

- **The cost of misclassifying is deployment-dependent, not absolute.** A setting in the wrong category is a real
  defect where tenants hold API access and a naming inconsistency where they do not.
- **So the mechanism should be data rather than code.** A compiled-in list of operator-only operations would be
  wrong for the provisioning-UI deployment, where an operator legitimately wants every one of them. A reserved
  prefix and a projected list of operator-owned resources are configuration, and a deployment that needs neither
  simply declares neither.
- **And it argues against building the privileged-caller notion first.** The gap at the end of §5 is real, but the
  deployment that most needs a separate operator surface is the one where tenants have no access, which is exactly
  the deployment that does not need the distinction enforced.



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
   scope file, and the route and group counts in the README change with them. Note the answer is "not on the
   tenant surface" rather than "nowhere" — under §5's companion clause an operator-facing surface could offer it,
   and there is no such surface today.
3. **Is weighted routing a tenant API at all?** §3 above. Answering "yes, with the dimension validated" is a
   position, and the one the supervisor has assumed.
4. **Reserved prefix for operator-registered resources: what is it, and does it apply per resource kind?** Needed
   before anything in §5 can be built, and it is a compatibility decision as much as a design one, since the prefix
   becomes a name tenants can never use.
5. **Is hidden (level 3) wanted at all, or is visible-and-read-only enough?** The precedent that exists in the wild
   is visible. Hidden costs the reserved prefix *and* a rule about what a collision reports, and nothing here needs
   it yet.
