# Where the hard parts are

A survey of the 73 stubbed operation groups, sorted by what makes them hard rather than by
namespace. Written to answer "what should we not underestimate?" — the goal is to avoid discovering
mid-implementation that an operation needs machinery we have not built.

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
- **`snapshot.get`, `snapshot.status`** — scan across snapshot records.

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
- **decommission awareness and weighted routing** (6 groups) — single key each, pure config.
- **`snapshot.create/get/delete_repository`** — one key per repository. `verify_repository` is not
  Tier 0: it does real I/O against the backend.
- **`ingest.processor_grok`** — returns OpenSearch's built-in grok pattern list. Trivial logic, but
  needs the pattern data copied across.
- **`indices.create_data_stream/get/delete`** — mostly CRUD, but data streams own a backing-index
  naming convention and a designated write index, so they are Tier 0 storage with Tier 2 rules
  layered on.

## Tier 4: orchestration

These need the task lifecycle to be real — attempt counts, leases, terminal-state sweeps — which the
current `IndexManagementTasks` explicitly is not.

- **`indices.clone`, `shrink`, `split`** — create target metadata, then orchestrate a data copy
  through the shard manager. Each has preconditions worth reproducing: the source must be
  read-only, shrink requires a single-shard target, split requires the shard count to be a multiple
  of the source's.
- **`indices.rollover`** — still the most demanding operation in the namespace, but less of it is
  ours than it first looked. Condition evaluation needs doc counts and sizes the control plane does
  not have, so it is a collaboration with the shard manager rather than something to implement here.
  The write-index alias switch is now a single-key update (see above). What is left for the control
  plane is implicit target naming (`logs-000001` → `logs-000002`), `dry_run`, and driving the
  exchange. Still last, but for orchestration reasons rather than atomicity ones.
- **`snapshot.create/restore/clone/delete`, `cleanup_repository`, `verify_repository`** — long-running
  work against a repository backend, plus progress reporting through `snapshot.status`.
- **`indices.open`, `close`** — state transitions that the shard manager must act on.

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
