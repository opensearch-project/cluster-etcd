# Findings from the control plane

Items the control plane (`../control-plane`) needs from this component, carried here by the user, in the form the
supervisor's `FINDINGS_FROM_THE_DATA_PLANE.md` uses: what is wanted, why, where the code is, what a change must be
careful about, and the tests worth having. Reply inline.

---

## 1. Publish document counts and store sizes per copy, so the cat tables can stop printing `x`

> **Open.** Nothing is blocked: `_cat/indices` and `_cat/shards` answer today with `x` in the document-count and
> store-size columns. This is the request that replaces the `x`.

### What the control plane is doing

`_cat/health`, `_cat/indices`, `_cat/shards` and `_cat/nodes` are answered by the control plane for a tenant, because
this component refuses `cluster.health` and `cluster.state` for one (the control plane's
`FINDINGS_FROM_THE_DATA_PLANE.md`, item 1) and each cat table calls one of them. They are views of what the supervisor
projects, and the supervisor projects placement and health, not statistics. Four columns have nowhere to come from:

| table | columns | upstream's default |
|---|---|---|
| `_cat/indices` | `docs.count`, `docs.deleted`, `store.size`, `pri.store.size` | all four shown |
| `_cat/shards` | `docs`, `store`, and `docs.deleted` on request | `docs` and `store` shown |

They print `x` for an open index, and blank for a closed one, which has no statistics upstream either. A column of
`x` is honest and useless: `_cat/indices?s=store.size:desc` is the first thing an operator reaches for, and a tenant
watching its own growth has nothing else to read.

### Where to publish them, and why not the inventory

**A document of its own, written by the node, in the tenant's key space:**

```
tenant/<t>/stats/<index-uuid>/<node-id>
```

```json
{
  "index_uuid": "uzKxLWZu4nPSrTknEcDpoA",
  "node_id": "pv1sDMfZRxSTMiW5VrbN9w",
  "shards": {
    "0": { "role": "PRIMARY",  "docs": 1204551, "docs_deleted": 3012, "store_bytes": 4823012 },
    "1": { "role": "REPLICA",  "docs": 1198770, "docs_deleted": 2877, "store_bytes": 4790221 }
  },
  "observed_at": 1788969977933
}
```

- **One writer per key.** The node owns the key, as it owns `nodes/heartbeat/<id>`. The control plane's store is
  confined to `tenant/<t>/`, so a tenant-prefixed key is the only place it can read, and the key is per node *and*
  per index so that no two nodes write the same document and a document stays small.
- **Not in the shard inventory.** `NODE_CONTRACT.md` says why in so many words: doc counts and per-shard statistics
  "would put an unbounded, fast-changing field into a document that is supposed to change only when placement does",
  and wants them in "a document of its own". This is it.
- **Not through the supervisor.** It would have to rewrite `status/indices/<uuid>` whenever a count moved, which is
  the write amplification that projection was built to avoid, and it would be a second hop for a figure the node
  already holds. If nodes should not write under `tenant/`, the fallback is `nodes/stats/<node-id>` with every tenant's
  figures in one document and the supervisor projecting it per tenant; that costs the supervisor a new projection and
  puts one tenant's numbers beside another's in a document no tenant reads. The first is simpler.

### What each figure is

Per copy, measured the way the shard inventory already measures `size_bytes`
(`IndicesServiceShardSizes`: `storeStats().sizeInBytes()`), so that the two cannot disagree:

- `docs` — `docStats().getCount()`, which is what `_cat/shards` prints as `docs` and excludes deleted documents.
- `docs_deleted` — `docStats().getDeleted()`.
- `store_bytes` — the same figure as `size_bytes`.
- `role` — what the node believes the copy is: `PRIMARY`, `REPLICA` or `SEARCH_REPLICA`.

Omit a figure for a copy that is initializing, as `size_bytes` is omitted, rather than writing zero: zero is a real
count, and the control plane will print `x` for an absent one. Report **per copy, never summed**. The sum is the
control plane's to take, and the next section is why.

### Why the node must not add them up

In vanilla OpenSearch a shard has one primary, so "the primaries' size" is a sum of one copy per shard and
`_cat/indices` can print it. **That is not true here.** A rollout can run two primaries of the same shard at once: a
dual-run writes to the old version and the new at the same time (the supervisor's `DESIGN.md` §29.4). Then:

- the documents in the shard are *the same documents*, held twice, so adding the two primaries' `docs` double-counts
  them;
- the bytes are really on two disks, so `store.size` should include both;
- "the primary's size" has two candidates, and which one a tenant means is not the node's to say.

So the control plane decides, by joining each figure to the supervisor's projection on `(node id, shard)`, which is
unique because a node holds at most one copy of a shard:

| figure | defined as |
|---|---|
| `docs.count`, `pri.store.size` | summed over each shard's **projected** primary, the `PRIMARY` entry in `status/indices`. During a dual-run that is the *old* primary, and the new one is in `shadow_primaries` (§29.4), which is never summed |
| `store.size` | summed over **every** observed copy, shadow primaries included, since that is the disk in use |
| any sum | printed `x` if any copy contributing to it has no figure newer than the staleness limit below, never as a partial sum that looks complete |

A node that publishes only what it holds, per copy, is all this needs. Nothing about the dual-run needs to be known
here, and nothing needs to change on this side when it ships.

### Cadence, size and lifetime

- **Write when a figure changes, no more than once every 30 s, and at least once every 5 minutes while the node is
  alive.** The count moves constantly under ingestion; a figure a half-minute old is the freshness `_cat` gives
  anyway. The 5-minute rewrite is the staleness signal: the control plane treats a document with `observed_at` older
  than 15 minutes as absent, and prints `x`. Leave the document untouched when nothing changed except
  `observed_at`, by the digest rule the inventory's `generated_at` follows, and rewrite it on the 5-minute beat.
- **Size.** About 80 bytes a copy, so a thousand copies of one index on one node is ~80 KiB, well inside the store's
  960 KiB value limit (`../kv-store/BACKENDS.md`). Past 512 KiB, write nothing and report it, as the supervisor does
  for a projection over budget.
- **Delete the document when the node stops holding any copy of the index**, including when the index is deleted. The
  control plane ignores a document for a copy the projection does not list, so a leftover is harmless, but it is a key
  that nobody will ever clean up.
- Additive fields are ignored by the control plane, as everywhere else.

### What a change should be careful about

- **Do not add a figure that changes faster than every 30 s** to this document, for the reason the inventory keeps
  these out. If search or indexing rates are wanted later, they want their own document.
- **A copy that moves nodes leaves a stale document behind** on the old node until it is deleted. The join on
  `(node id, shard)` makes that harmless, which is another reason not to key by shard alone.
- **A restarted node's old figures** should be rewritten, not trusted: the node's first write after a restart should
  not carry numbers measured before it.
- **Multi-tenant nodes** write one document per tenant per index, never a document spanning tenants.

### Tests worth having

- a started copy publishes its docs, deleted docs and bytes, and an initializing copy publishes none of them;
- an unchanged copy is not rewritten inside the 5-minute beat, and is rewritten on it with a newer `observed_at`;
- a changed count is written within 30 s and not more often than that;
- removing the last copy of an index from a node deletes its document;
- two tenants on one node get two documents, and neither mentions the other.

---

## Where these came from

Item 1 came from bringing the cat tables into the control plane. The data plane's refusal of `cluster.health` and
`cluster.state` for a tenant sends `_cat/health`, `_cat/indices`, `_cat/shards` and `_cat/nodes` there, and the first
of those that wants a number the projections do not carry is `docs.count`. The multi-primary point is the user's:
unlike vanilla OpenSearch, clusterless can run more than one primary for a shard, so the total for an index's
primaries needs to be defined by whoever reads the figures rather than assumed by whoever writes them.
