# Where the hard parts are

A survey of the 73 stubbed operation groups, sorted by what makes them hard rather than by
namespace. Written to answer "what should we not underestimate?" — the goal is to avoid discovering
mid-implementation that an operation needs machinery we have not built.

**The counts below are as of writing**, when 73 groups were stubbed; most have since been implemented or
struck through, and three left the surface altogether on the ownership axis. The README's status block is
the current number. The tiers are still the useful part — they say what each operation *needs*, which does
not change as the count falls.

Grounded in the spec and in OpenSearch 3.9.0 behaviour, not guessed. Where a claim is inference
rather than observation it says so.

## Summary

| tier | what makes it hard | count |
|---|---|---|
| 0 | nothing — single key, single document | ~21 |
| 1 | needs a scan or aggregation across keys | ~13 |
| 2 | non-trivial merge or composition semantics | ~8 |
| 3 | atomic mutation across several resources — **largely dissolved, see below** | ~10 |
| 4 | long-running orchestration and task lifecycle | ~14 |
| 5 | needs facts the control plane does not own | ~4 |

Tier 3 is the one that interacts badly with the per-resource-key design, and it is worth deciding
about before writing any of it.

---

## Tier 3 first: atomicity across resources

The per-resource-key design buys a lot. `indices.create` gets concurrency safety from a single
create-if-absent, with no read-modify-write of a shared blob and no contention between unrelated
indices. That is a real improvement over one monolithic cluster state.

The cost lands on operations that mutate several resources at once and are **atomic in OpenSearch
because it has one cluster state**.

**`indices.update_aliases` is the sharpest case.** Its body is a list of `actions`, and OpenSearch
applies the whole list as one cluster-state update — all of it or none. Worse, a single `add` action
takes `index` *or* `indices` (with wildcards) and `alias` *or* `aliases`, and `remove_index` deletes
an index outright. So one request can atomically mutate N index documents. The canonical use is a
zero-downtime alias swap:

```json
{"actions": [
  {"remove": {"index": "logs-v1", "alias": "logs"}},
  {"add":    {"index": "logs-v2", "alias": "logs"}}
]}
```

Applied non-atomically there is a window where `logs` points at nothing, or at both. Clients rely on
this being atomic; it is the documented way to do a reindex cutover.

Also in this tier, all multi-index: `indices.put_alias`, `indices.delete_alias`,
`indices.put_mapping`, ~~`indices.put_settings`~~, ~~`indices.open`~~, ~~`indices.close`~~,
`indices.add_block`.

### `indices.add_block` needs block enforcement first

Listed just above as multi-index, which is the Tier 3 half of its problem. Probing 3.9.0 on 2026-09-08
showed there is a second, larger one, and the operation was deferred on that basis.

Adding a block writes `index.blocks.<name>: true` and installs a cluster block. Two of the six blocks
carry `METADATA_WRITE`, and that level is enforced against **subsequent metadata operations**:

| block | id | levels | who enforces |
|---|---|---|---|
| `read_only` | 5 | `WRITE`, `METADATA_WRITE` | metadata half is ours |
| `metadata` | 9 | `METADATA_WRITE`, `METADATA_READ` | ours |
| `read` | 7 | `READ` | data plane |
| `write` | 8 | `WRITE` | data plane |
| `search_only` | — | `WRITE` | data plane |
| `read_only_allow_delete` | — | — | rejected outright: `read_only_allow_delete block is for internal use only` |

Verified: with a `metadata` block set, a further `add_block` **and** a `put_settings` both answer
`403 cluster_block_exception`, `index [x] blocked by: [FORBIDDEN/9/index metadata (api)];`. This service
enforces none of it — `index.blocks.*` is stored as an ordinary setting and nothing reads it — so
implementing `add_block` alone would store a block this service then ignores, and the operation could not
correctly refuse itself.

The scope answer follows the three-plane model rather than being a matter of taste: a `METADATA_WRITE`
block gates metadata operations, which is all this service does, so enforcing that level belongs here.
`READ` and `WRITE` blocks gate data traffic, so the data plane honours those by reading the setting. What
is missing is a shared check over the metadata write paths — `put_settings`, `put_mapping`, `delete`, the
alias operations, `close`/`open`, and `add_block` itself — plus the 403 shape. That is a cross-cutting
change to roughly eight handlers, which is why it is its own piece of work and not a rider on a small
operation.

There is one wrinkle to reproduce when it happens: OpenSearch deliberately lets a settings request
through if it contains **exactly one** setting and that setting is a block setting
(`TransportUpdateSettingsAction.checkBlock`), because otherwise a blocked index could never be unblocked.
Sending two block settings at once is refused; sending them one at a time works.

### Done: aliases are their own resource

Multi-key transactions are to be avoided, so aliases move out of index metadata and become
`aliases/<name>` holding the indices they point at plus their filter, routing and write-index
settings.

The payoff is larger than "one option among four": **it collapses the canonical alias swap to a
single key.** Remove `logs` from `logs-v1` and add it to `logs-v2` is not two writes to two index
documents — it is one update to `aliases/logs`, changing its index list from `[logs-v1]` to
`[logs-v2]`. That is a single-key compare-and-swap, genuinely atomic, on any backend. The operation
the API mainly exists for stops being a problem.

It also removes the scan in Tier 1: looking up an alias by name becomes a direct key read instead of
walking every index.

Implemented, and a test asserts the swap bumps exactly one key revision.

What remains non-atomic is a request whose actions span several *different* aliases, e.g. adding
`x` to one index and `y` to another in one call. That is a much weaker requirement, and it is
covered from the other direction: the shard manager assembles the configuration it pushes to
coordinator nodes, and that push is atomic. Since aliases matter for operation dispatch — which is
the coordinator nodes' job — a brief window where the control plane's own keys disagree is not
observable by anything routing requests.

So the control plane does not need cross-resource atomicity at all. It writes independent resources;
the layer below assembles a coherent snapshot. Deleting an index still has to clean up alias
membership, which is a second write, but one that is easy to make idempotent and to sweep.

Consequences to keep in mind:

- `KeyValueStore` needs no transaction support, which keeps object-store backends viable.
- Alias data is no longer co-located with the index, so index deletion and alias cleanup need a
  reconciliation path rather than relying on a single write.

---

## Tier 2: merge and composition semantics — port, do not reinvent

Copying `MetadataIndexTemplateService` is the right instinct. Notes on doing it:

- **It is coupled to types, not just to cluster state.** Beyond `ClusterState` and `Metadata` it
  wants `IndexMetadata`, `Settings`, `CompressedXContent`, `IndexScopedSettings`, and — for
  validation — `MapperService`. Swapping the state read for a store read is the easy part; deciding
  how far to follow the type graph is the real work. ~~`Settings` in particular is load-bearing and
  has its own flatten/normalise semantics that our current `index.`-prefixed map only approximates.~~
  **`Settings` is done** — `org.opensearch.controlplane.metadata.Settings` reproduces the
  flatten/normalise semantics, established by probing rather than by porting, and every template
  surface was diffed against a live cluster afterwards. See the README's Settings section.
- **`validateIndexTemplateV2` actually builds the mappings** to check them, which pulls in the
  mapping engine. That is likely the boundary worth stopping at: compose templates, but defer full
  mapping validation and accept that a bad mapping is caught later than a real cluster catches it.
- **Put-time validation is cross-resource.** OpenSearch rejects a new composable template whose
  priority and index patterns overlap ambiguously with an existing one, which means reading every
  other template. Not a merge problem, a scan problem — see Tier 1.
- **Licensing is fine but attribution matters.** OpenSearch is Apache 2.0 and so are we; copied
  files should keep OpenSearch's original copyright header rather than being relabelled to the
  OpenSearch Software Foundation.

Also in this tier:

- ~~**Legacy `_template` application**~~ — **done.** The survey filed this under composition, and the
  merge itself was the easy half. The load-bearing fact turned out to be that the two kinds are
  *alternatives, not layers*: a matching composable template suppresses every legacy one outright, so
  an index falls back to built-in defaults rather than to the legacy template that also matched. Legacy
  templates merge among themselves by ascending `order`, ties by name.
- ~~**`indices.put_mapping`**~~ — **done**, and cheaper than the survey feared. "The rules for what
  counts as a compatible update are subtle" is true of `MapperService.merge` as code, but the rules
  themselves are *declared*: every `Parameter` a field mapper builds carries an `updateable` flag, so
  the whole compatibility question is a transcribable list — 30 parameter names always immutable, 18
  always updateable, 3 varying by field type. Carrying the list beat porting the merge.

  The residual gap is defaults, not rules. A real cluster compares against the mapper's *resolved*
  value, so it can tell `index: true` on a text field (a no-op) from `index: false` (a conflict); we
  store mappings as sent and cannot, so we reject both. Stricter, never looser, and recorded.

  Tier 3 applies as predicted — it is multi-index — and the answer is to merge against every index
  before writing any, so a conflict on the last leaves the first untouched.
- **`indices.put_settings`** — needs a registry of which settings are static versus dynamic, because
  static settings can only change on a closed index. Not hard, but it needs data we do not have:
  the settings registry itself.
- ~~**`indices.simulate_template` / `simulate_index_template`**~~ — **done**, and they earned their keep:
  every precedence rule the composition engine implements was established by probing
  `_simulate_index` against a live cluster rather than by reading `MetadataIndexTemplateService`, since
  the composed output is directly observable and the source is large.

---

## Tier 1: reads that need a scan

The direct consequence of not having a prebuilt cluster-state view.

- ~~**`indices.get_alias`, `indices.exists_alias`**~~ — **done.** They answer **index-keyed** documents
  (`GET /_alias/x` returns `{"index": {"aliases": {"x": {}}}}`) so they need index→aliases, which stays
  a scan whatever the storage layout. `AliasStore.forIndex` implements it and records why denormalising
  to avoid it would reintroduce a second source of truth.
- ~~**`indices.get`, `get_mapping`, `get_settings`, `get_field_mapping`**~~ — **done**, along with
  `resolve_index`. They took index expressions — wildcards, `_all`, comma-separated lists, alias names,
  `expand_wildcards`, `ignore_unavailable` — and the prediction that writing that resolution once and
  carefully would pay for itself held: all four are now rendering plus `IndexExpressions`, which holds
  the resolve-or-render-the-error preamble they share.

  Three things the survey did not anticipate. `get_settings` needed a rule of its own — the `{name}`
  filter matches the full dotted key with no implicit `index.` prefix, and an index filtered down to
  nothing is dropped rather than rendered empty. `get_field_mapping` cannot report metadata fields
  (`_index`, `_routing`, …) because they come from the mapping engine's registry rather than from
  anything a user declared. And **`resolve_index` does not use the resolver at all**: it reports on
  names rather than resolving them, so an alias stays an alias, `_all` is a literal, and nothing is
  ever an error.
- **`indices.resolve_index`** — resolves an expression across indices, aliases *and* data streams
  simultaneously. Effectively a test of the resolver above.
- **`cluster.state`** — the API that wants the monolith back. It assembles a single document from
  every per-resource key, filtered by `metric` and `index`. Ironically the hardest read in the
  system precisely because of the design choice that makes writes easy. Worth checking who actually
  calls it before investing: it may be used mostly by tooling we do not need to satisfy.
- ~~**`snapshot.get`, `snapshot.status`**~~ — deferred; the management plane projects snapshot metadata
  into the shared store and this reads it from there. See the snapshot boundary section.

## Tier 0: the easy wins, roughly in order of ratio

Worth knowing which these are, because several are near-copies of code that already exists:

- ~~**`search_pipeline.put/get/delete`**~~ — **done.** They turned out to be identical to ingest in
  every branch, including the metadata-presence delete quirk and the error wording, so rather than
  copying, the handlers were extracted and parameterised by `PipelineKind` and are now instantiated
  twice. The single difference is that ingest requires `processors` while a search pipeline accepts
  `{}`.
- **stored scripts** (`put/get/delete_script`) — one key per script id.
  `get_script_context` / `get_script_languages` are static lists.
- ~~**component and index templates**~~ — **done**, all twelve groups across three kinds. They turned
  out to differ more than expected: legacy answers a name-keyed map and a bare `{}` on a miss, the
  newer kinds answer an array under a wrapper key and a structured error, and the required property and
  its error differ per kind. Only `delete`-missing is uniform. Composition remains Tier 2 and unbuilt;
  `composed_of` references and priority overlaps are unvalidated.
- ~~**`cluster.get/put_settings`**~~ — **done**, and "a single key, modulo the static/dynamic
  distinction" was right on both counts. The static/dynamic question was answered by the settings
  registry rather than by a hand-built list, and the single key made the update a compare-and-swap. The
  only thing the survey missed is that the PUT response echoes the request rather than the state.
- ~~**weighted routing**~~ (3 groups) — **done**, and "single key, pure config" was right about the
  storage and wrong about everything around it. `{attribute}` looks like a resource identifier and is
  not: there is one document, so a `PUT` for a second attribute replaces the first and a `GET` ignores
  its own path parameter. It also carries a client-visible `_version` — the first optimistic-concurrency
  token in this service, sitting alongside the store's own revision rather than replacing it — and its
  validation depends on the *values* of two cluster settings, which made it the first reader of the
  settings store as configuration. The half of the rule that needs the live node list is deliberately
  not checked; see the README.
- **decommission awareness** (3 groups) — CRUD-shaped, but `put` initiates a node drain, so it is
  Tier 4 work in a Tier 0 costume. **Ownership makes the tier moot**: a node drain is the machines and
  the timing of changes to them, so it is the operator's at any tier, and the right outcome is exclusion
  rather than implementation. See [`tenants-and-operators.md`](tenants-and-operators.md).
- **`snapshot.create/get/delete_repository`** — one key per repository. `verify_repository` is not
  Tier 0: it does real I/O against the backend.
- **`ingest.processor_grok`** — returns OpenSearch's built-in grok pattern list. Trivial logic, but
  needs the pattern data copied across.
- ~~**`indices.create_data_stream/delete_data_stream`**~~ — **done**, and "Tier 0 storage with Tier 2
  rules layered on" was right, though the Tier 2 rules turned out to be borrowed rather than new: the
  hard part is the composable template, which was already built. A stream cannot exist without a matching
  template carrying a `data_stream` block, so `findV2Template` — `TemplateComposition.matchingIndexTemplate`
  here — is a hard prerequisite, and the template is matched against the *stream* name while the index is
  written under the backing name. The backing index is an ordinary index document, which is what lets
  every existing index read serve it for free.
  **`get_data_stream` is not done**: its required `status` field is health over the backing indices, so it
  is blocked on the same thing `cluster.health` is.

## Tier 4: orchestration

These need the task lifecycle to be real — attempt counts, leases, terminal-state sweeps — which the
current `IndexManagementTasks` explicitly is not.

- ~~**`indices.clone`, `shrink`, `split`**~~ — **out of scope as of 2026-09-21**, on the ownership axis
  rather than this one: a tenant cannot trigger them, because they are operator-initiated. `shrink` and
  `split` want hardware provisioned for the purpose rather than nodes serving queries, and shard count is
  supply rather than a tenant's demand. See [`tenants-and-operators.md`](tenants-and-operators.md).
  The preconditions stay worth knowing for whoever implements them on the operator side — the source must
  be read-only, shrink requires a single-shard target, and split requires the shard count to be a multiple
  of the source's — and the shape of the work does not change, only who may ask for it.
- **`indices.rollover`** — still the most demanding operation in the namespace, but less of it is
  ours than it first looked. Condition evaluation needs doc counts and sizes the control plane does
  not have, so it is a collaboration with the shard manager rather than something to implement here.
  The write-index alias switch is now a single-key update (see above). What is left for the control
  plane is implicit target naming (`logs-000001` → `logs-000002`), `dry_run`, and driving the
  exchange. Still last, but for orchestration reasons rather than atomicity ones.
### The snapshot boundary: three planes, not two

The first cut of this asked "data plane or control plane?" and got a good answer to the wrong question.
The real split has a middle:

| plane | snapshot work | why |
|---|---|---|
| **control plane** | repository **registration** — `create/get/delete_repository` | a single key; completes within the request |
| **management plane** | `cleanup_repository`, `clone`, `delete`, `verify_repository`, and the progress reporting behind `status` | long-running jobs against object storage, needing retries and leases |
| **data plane** | `create`, `restore` | move shard data to and from running nodes |

**"Not the data plane" does not make something the control plane's.** That is the correction. Cleaning up
a repository touches no running data node, so the two-plane version put it here — but it is a
long-running job, and this service does short-lived CRUD only. Same for cloning a snapshot: it copies
blobs, which is neither a data node's work nor a request-scoped operation.

`snapshot.create` and `snapshot.restore` remain out of scope for the original reason: they move shard
data on running nodes. OpenSearch treats them as cluster-manager operations, and that is the part we
deliberately disagree with — it is *why* they are long-running and awkward there.

#### What this buys: no second outbound seam

The two-plane version implied one. `RepositoryData` — which snapshots exist, their state, which indices
each holds — lives in `index-N` blobs **inside the repository**, not in cluster state, so every snapshot
operation except registration is a repository read. Putting those here would have meant a
`SnapshotRepository` interface alongside `KeyValueStore` and an S3/Azure/GCS dependency surface, breaking
the property that the key/value store is this service's only outbound seam.

Moving them to the management plane **removes the problem rather than solving it.** The management plane
is where repository credentials and clients belong anyway: it is the layer that already needs to talk to
infrastructure to allocate shards.

#### Decided: the management plane projects snapshot metadata into the shared store

`snapshot.get` and `snapshot.status` are reads, so they look like control plane work — but both read the
*repository*, which would reintroduce the seam. **The management plane reads the repository and projects
snapshot metadata into the shared store; this service reads it from there.** Same shape of answer as the
capabilities file and the settings registry, and it keeps the one-outbound-seam property intact.

Two things that makes better rather than merely tolerable:

- **The repository-read code can be reused from OpenSearch**, and it stays out of this repository. That
  is a good reason to put the read where the reuse is easy rather than reimplementing blob parsing here.
- **It is a deliberate improvement on the existing snapshot model.** Today the authoritative list of
  snapshots is only in the repository, so reading it means a blob round trip on every call and there is
  no way to query across repositories. Projecting it into the store makes snapshot metadata queryable
  like everything else.

`status` still spans planes: it reports in-flight progress, and `create`/`restore` run on the data plane
while `clone`/`delete` run on the management plane. Whatever answers it needs both, or answers partially
and says so — worth settling when the management plane exists to have the conversation with.

#### Done: the slice that needs none of that

~~`snapshot.create_repository`, `get_repository`, `delete_repository`~~ — **done.** Repository
**configuration**: a name, a type, a settings block. One key per repository, no repository access, no new
dependency, and independent of every decision above.

The behaviour was not as simple as the shape. `GET /_snapshot/foo*` is a **404 on a cluster that has
never had a repository and a 200 on one that has** — `TransportGetRepositoriesAction` takes an early
branch when `RepositoriesMetadata` is absent, naming the first token verbatim, and the normal path lets a
pattern matching nothing contribute nothing. That is the second instance of this artifact in the API
after `IngestMetadata`, and probing an empty cluster shows only one side of it. `DELETE` follows different
rules again: a single glob rather than a comma list, with `*` exempt from the 404 where `foo*` is not.
- ~~**`indices.open`, `close`**~~ — **done.** State transitions the shard manager must act on, so each
  writes a task; the response asymmetries and the closed-index settings rules are on
  `ChangeIndexStateHandler` and `PutIndexSettingsHandler`.

## Tier 5: facts we do not own

- **`cluster.health`** — green/yellow/red is derived from shard allocation state, which lives with
  the shard manager. Needs a defined channel for that, or an honest degraded answer.
- **`dangling_indices.*`** (3 groups) — inherently cross-layer: a dangling index is one a data node
  has on disk that the control plane has no metadata for. Requires node reports.
- **`indices.rollover` conditions** — as above.

## Plugin availability cuts across every tier

Discovered while implementing search pipelines, and it applies far beyond them. The spec is the union
of the whole distribution, so a value that is spec-valid may not exist in a given deployment. Pipeline
processors were the first instance; the same is true of index settings, analyzers, tokenizers, and
snapshot repository types, all of which plugins extend.

`DataPlaneCapabilities` handles processors today. Each of those other cases will need the same
treatment, and the same question answered: the control plane cannot know what the data plane has
unless it is told. Declared config is the stopgap; the durable answer is the shard manager publishing
capabilities into the store, since it is already the layer that talks to both sides.

Worth doing before implementing `indices.put_settings`, which will otherwise accept plugin settings
that the data plane silently ignores — a worse failure than rejection, because nothing surfaces it.

## Two inversions worth noticing

**`cluster.pending_tasks` gets easier, not harder.** In OpenSearch it exposes cluster-manager
internals. Here it is a read over our own `tasks/index-management/` prefix — arguably the most
natural API in the whole set for this architecture, and a good early way to make the task queue
observable without committing to the `_tasks` ID format.

**`cluster.post_voting_config_exclusions` / `delete_voting_config_exclusions` are now out of scope.**
They manage the cluster-manager quorum. This service is stateless and holds no state outside the
data store, so it has no quorum of its own; whatever consensus exists belongs to etcd or DynamoDB.
Implementing them faithfully is not possible and pretending would be worse than a clear error.

---

## Statelessness, and what it forbids

The control plane is a stateless service: it holds nothing outside the data store, scales by running
more instances, and should be able to scale to zero on something like Lambda, since control plane
operations are infrequent and not latency sensitive.

Audited against the current code, and it holds:

- No mutable static fields anywhere.
- Three non-final instance fields, all in `NettyHttpServer` and all transport lifecycle
  (`bossGroup`, `workerGroup`, `channel`), plus `PathTrie.Node.wildcardChild`, which is written while
  the trie is built and read-only afterwards.
- No background threads, executors or timers beyond a shutdown hook. This is the property that makes
  scale-to-zero possible, and it holds because task *processing* belongs to the shard manager. If the
  control plane ever grows a polling loop it stops being able to scale to zero — worth treating as a
  design constraint rather than an accident.
- `cluster_uuid` is generated with create-if-absent, so concurrent cold starts converge on one value
  rather than racing.

Two things this rules out or complicates:

- **`FileSystemKeyValueStore` cannot back more than one instance.** Its compare-and-swap is an
  in-process lock, so two instances over one directory will silently lose updates. That was written
  up as a limitation; under this scaling model it is a hard boundary. It is a development and
  single-instance store, and multi-instance requires etcd or DynamoDB. Nothing warns about this at
  startup, which it probably should.
- **Netty is the wrong front end for Lambda.** Not a problem to fix now: `HttpRequestDispatcher` is
  the seam, and the routing and handler layers know nothing about Netty, so an invocation-based
  entry point is an additive change. Worth noting that `ClusterUuid.resolve` does a store read on
  every `GET /`, which under Lambda is a round-trip per call; it is immutable once written, so it can
  be cached per instance when that matters.
