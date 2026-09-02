# Where the hard parts are

A survey of the 73 stubbed operation groups, sorted by what makes them hard rather than by
namespace. Written to answer "what should we not underestimate?" — the goal is to avoid discovering
mid-implementation that an operation needs machinery we have not built.

Grounded in the spec and in OpenSearch 3.9.0 behaviour, not guessed. Where a claim is inference
rather than observation it says so.

## Summary

| tier | what makes it hard | count |
|---|---|---|
| 0 | nothing — single key, single document | ~24 |
| 1 | needs a scan or aggregation across keys | ~13 |
| 2 | non-trivial merge or composition semantics | ~8 |
| 3 | **atomic mutation across several resources** | ~10 |
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
`indices.put_mapping`, `indices.put_settings`, `indices.open`, `indices.close`,
`indices.add_block`.

### Options, with what each costs

1. **Multi-key transactions in `KeyValueStore`.** etcd has `Txn` with compare-and-swap over multiple
   keys; DynamoDB has `TransactWriteItems` (100 items, and it fails the whole transaction on any
   conflict); **S3 has nothing equivalent**. So this option decides the backend question — it rules
   out object stores for good, which is worth knowing now rather than after picking one.
2. **A serialising key for the mutation class.** Alias mutations take a CAS on a single
   `aliases/generation` key, making them mutually exclusive without needing multi-key atomicity.
   Cheap, works on any backend, but serialises all alias changes cluster-wide and still leaves a
   window where a reader sees a half-applied change unless readers also check the generation.
3. **Aliases as their own resources** rather than fields inside index metadata: `aliases/<name>`
   holding the list of indices it points at. Then an alias swap is a single-key CAS and genuinely
   atomic. This is the most attractive option and the one I would look at first — it changes the
   data model rather than the store contract. The cost is that alias data is no longer co-located
   with the index, so deleting an index must also clean up its aliases, which is itself a multi-key
   write (though a much easier one to make idempotent and sweep).
4. **Accept non-atomic and document it.** Defensible for single-index cases; not defensible for the
   swap above, which is the main reason the API exists.

Option 3 plus option 1 for the residue is probably where this lands, but it is a decision, and
`indices.update_aliases` should not be started before it is made.

---

## Tier 2: merge and composition semantics — port, do not reinvent

Copying `MetadataIndexTemplateService` is the right instinct. Notes on doing it:

- **It is coupled to types, not just to cluster state.** Beyond `ClusterState` and `Metadata` it
  wants `IndexMetadata`, `Settings`, `CompressedXContent`, `IndexScopedSettings`, and — for
  validation — `MapperService`. Swapping the state read for a store read is the easy part; deciding
  how far to follow the type graph is the real work. `Settings` in particular is load-bearing and
  has its own flatten/normalise semantics that our current `index.`-prefixed map only approximates.
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

- **`indices.put_mapping`** — mapping merge with conflict detection: a field's type cannot change,
  and the rules for what counts as a compatible update are subtle. `MapperService.merge`. Deep, and
  multi-index, so it is Tier 2 *and* Tier 3.
- **`indices.put_settings`** — needs a registry of which settings are static versus dynamic, because
  static settings can only change on a closed index. Not hard, but it needs data we do not have:
  the settings registry itself.
- **`indices.simulate_template` / `simulate_index_template`** — pure functions over the composition
  engine, so they come almost free once it exists. Good early validation that the port is correct,
  and they have conformance stories.

---

## Tier 1: reads that need a scan

The direct consequence of not having a prebuilt cluster-state view.

- **`indices.get_alias`, `indices.exists_alias`** — aliases currently live *inside* index metadata,
  so a lookup by alias name means scanning every index. OpenSearch answers from a prebuilt lookup
  map. This is the same finding that motivates option 3 above: making aliases their own resource
  fixes both the atomicity problem and this one.
- **`indices.get`, `get_mapping`, `get_settings`, `get_field_mapping`** — all take index
  expressions: wildcards, `_all`, comma-separated lists, alias names, plus `expand_wildcards`
  (open/closed/hidden/none) and `ignore_unavailable`. That resolution logic is shared and should be
  written once, carefully, as its own component. It is the single most reused piece of logic in the
  `indices` namespace and getting it wrong is subtly wrong everywhere.
- **`indices.resolve_index`** — resolves an expression across indices, aliases *and* data streams
  simultaneously. Effectively a test of the resolver above.
- **`cluster.state`** — the API that wants the monolith back. It assembles a single document from
  every per-resource key, filtered by `metric` and `index`. Ironically the hardest read in the
  system precisely because of the design choice that makes writes easy. Worth checking who actually
  calls it before investing: it may be used mostly by tooling we do not need to satisfy.
- **`snapshot.get`, `snapshot.status`** — scan across snapshot records.

## Tier 0: the easy wins, roughly in order of ratio

Worth knowing which these are, because several are near-copies of code that already exists:

- **`search_pipeline.put/get/delete`** — structurally identical to the ingest pipeline handlers
  already written, including the wildcard and comma-list resolution. Close to a copy with a
  different key prefix. Best next task if the goal is momentum.
- **stored scripts** (`put/get/delete_script`) — one key per script id.
  `get_script_context` / `get_script_languages` are static lists.
- **component and index templates** (`put/get/delete/exists` × both) — one key per template. The
  *storage* is Tier 0; only `put` validation and any use of them is Tier 2, so the CRUD can land
  first and composition can follow.
- **`cluster.get/put_settings`** — a single key, modulo the static/dynamic distinction.
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
- **`indices.rollover`** — the most demanding operation in the namespace. It combines
  condition evaluation (Tier 5: `max_docs` and `max_size` need doc counts and sizes the control
  plane does not have), implicit target naming (`logs-000001` → `logs-000002`), an atomic write-index
  alias switch (Tier 3), and a `dry_run` mode. Everything hard in one endpoint. Leave it last.
- **`snapshot.create/restore/clone/delete`, `cleanup_repository`, `verify_repository`** — long-running
  work against a repository backend, plus progress reporting through `snapshot.status`.
- **`indices.open`, `close`** — state transitions that the shard manager must act on.

## Tier 5: facts we do not own

- **`cluster.health`** — green/yellow/red is derived from shard allocation state, which lives with
  the shard manager. Needs a defined channel for that, or an honest degraded answer.
- **`dangling_indices.*`** (3 groups) — inherently cross-layer: a dangling index is one a data node
  has on disk that the control plane has no metadata for. Requires node reports.
- **`indices.rollover` conditions** — as above.

## Two inversions worth noticing

**`cluster.pending_tasks` gets easier, not harder.** In OpenSearch it exposes cluster-manager
internals. Here it is a read over our own `tasks/index-management/` prefix — arguably the most
natural API in the whole set for this architecture, and a good early way to make the task queue
observable without committing to the `_tasks` ID format.

**`cluster.post_voting_config_exclusions` / `delete_voting_config_exclusions` may not be meaningful.**
They manage the cluster-manager quorum. A standalone control plane has no quorum in OpenSearch's
sense; whatever consensus exists belongs to etcd or DynamoDB. These are in scope today, but
implementing them faithfully may be impossible and pretending may be worse than a clear error. Worth
revisiting their scope classification.
