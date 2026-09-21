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

## The product this is for, and why it decides the classification

Every call below depends on which product this is, so it is worth stating before the sweep rather than leaving it
implicit. Two managed offerings sit at different points and the difference is not cosmetic:

- **Amazon OpenSearch Service** runs a *shared responsibility* model. The service replaces failed hardware, takes
  automated snapshots, and retries failed allocations — but **the customer chooses and pays for specific instance
  types**, and remains free to put too much data in a shard, create too many indices, or send too much traffic.
- **OpenSearch Serverless** does not ask the customer about hardware at all.

**This project targets the second shape.** The operators run pools of nodes on hardware they own, and a customer
states what they need rather than what to run it on. An autoscaling component is intended eventually — reacting to
signals from the supervisor and the monitoring plane, ordering hardware, and letting the supervisor spread shards,
add search replicas, and in time initiate a live shard split. It is not scoped yet, but it is the direction the
classification should anticipate, because it moves the boundary.

Two qualifications that matter as much as the shape:

**The tenant keeps real authority, not a reduced menu.** They create their own indices with their own mappings to
solve their own business problems. They know their business and want search as a solution; the operators know
OpenSearch and want running that solution to be painless. So this axis is not about withholding capability — it is
about not making a tenant answer questions about hardware they did not buy.

**And the audience is internal, not a public cloud.** The target is a Search Platform team at a reasonably sized
company operating this as a service for product and business teams. That is why *"customers shouldn't care where
their data lives"* is the goal rather than *"shouldn't know"*: a customer who sees behind the curtain has found
something not useful to them, not something they were denied. **Secrecy is not a design goal here**, which settles
more below than it sounds like it should.

### What follows from it

- **The cutting line for capacity is supply against demand.** The operator owns supply — nodes, hardware, placement,
  snapshots, self-healing, and eventually the autoscaler. The tenant owns demand — indices, mappings, documents,
  queries, and the freedom to overload themselves. That is a sharper tool than §25.1 for anything with a number in
  it, because it says who is *accountable* for the number rather than who may write it.
- **Autoscaling changes the remedy for the intersection category.** §25.2 resolves tenant-intent-against-
  operator-capability by honouring the request up to the capability and *reporting* the shortfall. With an
  autoscaler in the loop the report stops being a message to a human and becomes **an input that changes the
  capability** — the same finding, two audiences. Worth saying to the supervisor before the findings vocabulary
  hardens: a finding wants a machine-consumable payload (which resource, how far short) and not only prose, because
  something will act on it.
- **Secrecy-motivated filtering is not worth building.** The awareness narrowing gave two reasons, and its second —
  that a tenant-scoped document was telling every tenant in a shared pool the name of the repository the pool writes
  to — is worth approximately nothing under this model. The first reason carries it alone, and comfortably: an
  attribute that is not a routing dimension should not be offered as one. Good outcome, since correctness-motivated
  filtering stays correct when the threat model changes and secrecy-motivated filtering does not.
- **Placement detail may be visible without becoming a contract.** "Shouldn't care" permits exposing where data
  lives; it does not permit a tenant building on it. So an eventual `cluster.state`-shaped projection is fine, and
  the thing to avoid is a response field that quietly becomes a promise about a node that the autoscaler will later
  break.

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

Eight items, most-actionable first. The last is not an exception so much as the reason the others are
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

**The product shape makes this firmer than "a known instance".** In a managed-hardware offering a customer who
chose and paid for particular instances has some claim to say which of them their index sits on. Here nobody is
buying specific nodes, so `require.zone: z1` is not a request that can be honoured *or* meaningfully refused — it
names a fleet the tenant does not have a view of, and an autoscaler will move the ground under it. Rejection is the
right end state; it is queued behind the same missing mechanism as §1 rather than being a separate decision.

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

#### Two access levels. Hidden was considered and dropped

1. **Tenant-owned** — full CRUD. Today's only mode.
2. **Operator-owned, visible** — present in reads; writes and deletes rejected. This is the automated-snapshot
   shape, and it is the answer: a tenant that can list and restore from the operator's repository can use it, which
   is the point of it existing.

A third level — operator-owned and **invisible** — is not being built. The costs are real and the benefit is
negligible to non-existent for an internal service where seeing behind the curtain is acceptable. Recorded because
the reasoning outlives the decision, and because a public-cloud version of this would have to revisit it.

Invisibility and name collision are in **direct tension.** If the operator has a hidden repository `backups` and a
tenant sends `PUT /_snapshot/backups`, every answer is bad:

- *accept* → two resources under one name, and whichever loses resolution is silently unreachable;
- *reject* → the rejection discloses the existence of the thing that was meant to be invisible, and the message has
  to either say why or lie;
- *accept and shadow* → the tenant's registration works and the operator's stops being used, so a guarantee the
  operator made (backups keep running) breaks on a tenant write.

The escape would be **reserved names**, which is what Amazon OpenSearch Service does — the automated repository sits
at a reserved name (`cs-automated`, plus an encrypted variant) that a customer's registration cannot take, so
collision is impossible by construction rather than adjudicated. Note it is *visible* there rather than hidden,
which was the first hint that level 2 is the whole of the useful part.

**What survives from all that is the collision problem, which level 2 has too.** An operator declaring `backups`
while a tenant already has one is still two resources under one name, and it still needs an answer — but with
nothing to conceal, the answer is easy: reject the tenant's write with a message that names the operator's
registration and says it cannot be replaced. A naming convention is then a convenience for making that message
obvious rather than a requirement for making the design coherent.

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

### 6. Per-tenant quotas are the operator-owned constraint this service is missing entirely

"Tenants remain free to shoot themselves in the foot" is right, and in a shared pool some of those feet belong to
neighbours. Too much data in one shard is the tenant's own problem; **too many indices is the pool's**, and there is
no mechanism here for an operator to bound it — nothing in this service reads a limit of any kind, verified rather
than recalled.

This is the same mechanism as §5 rather than a new one, which is the argument for noticing it now: the limit is the
operator's to declare, the write path it must be enforced on is ours, and the route between them is the one already
built — declared on the pool, projected per tenant, read here at creation time. Three notes on how it differs from
awareness:

- **Absence fails open, and that is the right default.** No projected quota means unlimited, which is today's
  behaviour, so the mechanism can land before any operator declares anything.
- **Enforcement belongs here even though the accounting does not.** Index count is answerable from a prefix scan of
  our own keys; shard count against live capacity is not, and that half stays the supervisor's.
- **The error shape exists already.** OpenSearch has a validation failure for exceeding
  `cluster.max_shards_per_node`, so a quota rejection has a precedent to match rather than inventing one.

Nothing to build until an operator needs it, but it is the most likely next item on this axis, and it wants deciding
alongside §5 because the two would otherwise grow two different projections for the same purpose.

### 7. Shard count is a capacity decision the tenant is currently required to make

The clearest case of vanilla making a tenant answer a question this model says is the operator's.
`index.number_of_shards` is set by the tenant at creation, static thereafter, and OpenSearch Serverless does not ask
a customer for it at all. Under *supply against demand* it is squarely supply: how the tenant's data is spread over
hardware they do not own.

Two things make it more than a curiosity:

- **The autoscaler makes it a moving value, not just a misplaced one.** Once live shard split exists, shard count is
  something the system should choose and then revise as the data grows. A tenant-declared static number is then not
  merely in the wrong hands, it is a value the operator must silently overrule to do its job — and §25.2's remedy
  (honour up to the capability, report the shortfall) does not fit a number that is *supposed* to change.
- **The resize family inherits the question, and it is now answered.** **Decided 2026-09-21: tenants cannot call
  `indices.clone` / `shrink` / `split` directly.** They are operator-initiated — by a human operator or eventually
  by the autoscaling service — and the three groups have left the tenant surface in the scope file. Two reasons,
  and the second is the one that generalises: `shrink` and `split` are heavy enough that this system intends to run
  them on hardware provisioned for the purpose rather than on nodes serving queries, and shard count is supply
  rather than demand. Whether an operator or the autoscaler schedules the work is an implementation detail; that it
  is *scheduled* rather than triggered by whoever asked is not. A tenant who wants a split still gets one by asking
  — what is removed is the direct trigger, not the outcome. `indices.rollover` stays in scope: index lifecycle, not
  shard-count surgery, and it needs no hardware anyone has to provision.

Three things that decision drags behind it, all recorded rather than solved:

- **The rejection cannot explain itself.** An excluded operation has no route, so these now answer the generic
  `400 no handler found` — honest, and no help at all to a tenant wondering who to ask. The alternative, a route
  kept alive solely to return a pointed message, claims the API exists and then refuses it, and costs a handler.
  Worth revisiting the moment there is an operator-facing surface to point *at*; until then the generic answer is
  the one that does not lie.
- **The desired-state write still has to come through here, which is a feature.** An operator-initiated split
  produces a *new index*, and its `indices/<name>` document is a key whose only writer is the control plane —
  `KEY_LAYOUT.md`'s one-writer invariant, and §5.5 forbids the supervisor writing it. So the mechanics are
  "whoever initiates creates the target through the ordinary create path, and the supervisor does the copy", not
  "the supervisor invents an index". That needs no new key-space rule and no privileged caller: the target index is
  an ordinary index, and only the *trigger* left the tenant surface. Had the resize been implemented here first,
  this would have been discovered as a contract violation instead.
- **Dedicated hardware is a scheduling constraint the task record would have to carry.** "Not on nodes serving
  queries" is a placement decision, so whatever eventually initiates a split has to say *where* it may run — a node
  role or a separate pool. Nothing in this service needs to know that, which is the point of noting it: the
  constraint lives entirely on the operator side, and the control plane's ignorance of it is now correct rather
  than a gap.
- **And the operation a tenant would have been calling is not the operation this system performs.** Checked in the
  supervisor's `NODE_CONTRACT.md` rather than assumed: in-place live shard splitting is explicitly *not* the
  approach, because pull-based ingestion makes a split a matter of standing up a new index with the new shard
  layout, restoring from snapshot, deleting the out-of-shard documents, and resuming from the checkpoint in the
  shard snapshot — so OpenSearch's `SPLITTING` shard state should never appear in a pool this system owns.

  Two things follow. First, `POST /{index}/_split` would have been a tenant-facing verb for a procedure with
  different steps, different intermediate states and a different failure surface: excluding it avoids promising a
  vanilla shape over machinery that does not match it, which is a stronger reason than ownership on its own.
  Second, **that procedure is assembled from primitives this service already exposes** — create an index, restore,
  delete by query, and resume ingestion from an offset. The `resume-ingestion` reset written earlier is precisely
  the "resume from the checkpoint" step. So the operator surface needed for a split may be orchestration over
  existing operations rather than a new API, and the pause/resume semantics are load-bearing in an operator
  procedure rather than only a tenant convenience — which raises the cost of the one weakness already recorded
  about them, that a lost `details.reset_settings` payload is not recoverable from anywhere else.

Not proposing a change. `index.number_of_shards` is load-bearing in every client and tool, so removing it is a
compatibility decision well beyond this axis, and accepting-then-overruling it is a defensible landing place. Listed
because the decision belongs to whoever builds the autoscaler, and it is easier made now than discovered then.

### 8. How much of any of this matters depends on whether tenants reach this service at all

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
   scope file, and the route and group counts in the README change with them — §7's exclusion is the worked
   precedent, and it cost one scope-file edit plus four stale numbers. Note the answer is "not on the
   tenant surface" rather than "nowhere" — under §5's companion clause an operator-facing surface could offer it,
   and there is no such surface today.
3. **Is weighted routing a tenant API at all?** §3 above. Answering "yes, with the dimension validated" is a
   position, and the one the supervisor has assumed.
4. **What does a name collision on an operator-registered resource report?** All that survives of the reserved-name
   question now that invisibility is dropped (§5). A message naming the operator's registration is enough; a naming
   convention is a convenience rather than a prerequisite.
5. ~~**Is hidden wanted?**~~ **Answered: no.** Visible and read-only is the whole of it — the costs are real and the
   benefit is negligible for an internal service. A public-cloud version would have to reopen it.
6. **Do quotas and operator-registered resources share one projection?** §6. Deciding it late means two mechanisms
   for one purpose.
7. ~~**Who owns shard count and the resize family?**~~ **The resize family is answered: operator-initiated, and
   `clone`/`shrink`/`split` are out of the tenant surface as of 2026-09-21** (§7). `index.number_of_shards` itself
   is still tenant-set and still belongs to whoever scopes the autoscaler; accept-then-overrule is the likely
   landing place.
