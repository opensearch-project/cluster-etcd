# Findings from the data plane

Items the data plane (`../data-plane`) needs from this component, written there and carried here by the user, in the
form the supervisor's file of the same name uses: what happened, why, where the code is, what a change must be careful
about, and the tests worth having. Reply inline.

---

## 1. `cluster.health` and `cluster.state` for a tenant are this component's to answer

> **Done**, apart from the figures noted below. `cluster.health`, `cluster.state` and the four cat tables
> (`_cat/health`, `_cat/indices`, `_cat/shards`, `_cat/nodes`) are answered here.
>
> - **Figures are the supervisor's**, as asked. Each index's status and counts come from its
>   `status/indices/<uuid>` document, never recomputed from the observed copies. Cluster figures are the sum over
>   the selected indices. `level=shards` applies the supervisor's own per-shard rule from
>   `StatusComputation.indexStatus` to the same placements, so a shard never disagrees with its index.
> - **Tenant names only.** Indices are listed by the names in this service's `indices/<name>` documents, matched
>   to projections by uuid, so only the tenant's own indices appear. An index not yet projected is red with every
>   copy unassigned; a projection for a deleted index is ignored.
> - **Waiting works.** `wait_for_status`, `wait_for_nodes` (every upstream operator), `wait_for_active_shards`,
>   `wait_for_no_initializing_shards`, and a named index all re-read the projections until they hold or `timeout`
>   passes, then answer 408 with the full body. That needed dispatch off the Netty event loop, which it now is.
> - **Staleness** needed the supervisor to rewrite `status/cluster` on a heartbeat, which it now does (its
>   `FINDINGS_FROM_THE_CONTROL_PLANE.md`, item 1). Absent, or older than `heartbeat_ms` + 35 s, is answered as
>   upstream answers no cluster manager: wait `cluster_manager_timeout`, then `503
>   cluster_manager_not_discovered_exception`; with `local=true`, the last figures, red.
> - **Not covered:** `level=awareness_attributes` is refused, since its per-value counts and weights are not
>   projected. Document counts and store sizes for `_cat/indices` are still open, as you say.
>
> All three tests asked for are in place: the missing replica and the tenant's own names in
> `ClusterHealthHandlerTests`, and the node list in `ClusterStateHandlerTests` (`anotherTenantsNodesAreNotListed`).
>
> **`cluster.state`:**
>
> - `nodes` is exactly the tenant's `status/nodes` documents, name, ephemeral id, transport address and
>   attributes, as a cluster state writes them. A lost node is in neither `status/nodes` nor `number_of_nodes`
>   (the supervisor leaves it out of both, and its copies out of `status/indices`), so health and state agree.
> - `routing_table` and `routing_nodes` come from `status/indices`. A copy a node reports `FAILED` is rendered
>   `UNASSIGNED`, and each copy an index asks for that nothing holds is added as one, so the sections agree with
>   `cluster.health`. Neither carries `unassigned_info` or `recovery_source`: when, why and from where are not
>   projected.
> - `metadata`, `blocks` and `version` need no supervisor, so the commonest request, an index's mappings and
>   settings, answers without one. Only `nodes` and the routing sections wait for a fresh `status/cluster`.
> - **Checked end to end** against a real store: the control plane's `cluster.state` over the supervisor's real
>   projections agrees with the data plane node's own serialisation on field order, node ids, shard copies and
>   allocation ids. The only differences are the known ones, plus the fourth copy of an index that wants three
>   replicas on three nodes, which the node does not fabricate and this shows as unassigned.
> - **Not covered:** per-index `primary_terms`, by decision: a tenant has no use for the term, and an operator can
>   have an API of their own. The supervisor's finding 2 is declined for that reason.
>
> **The cat tables**, as the finding asked, with the proxy routing all four here:
>
> - `_cat/health` is `cluster.health`'s figures, read by the same code; `_cat/shards` is `cluster.state`'s
>   routing, one row per copy, with the copies an index wants and nothing holds as `UNASSIGNED` rows; `_cat/nodes`
>   is `status/nodes`, so only the tenant's own nodes.
> - **Document counts and store sizes are `x`**, which the finding named as open. The request for them is
>   [`FINDINGS_FROM_THE_CONTROL_PLANE.md`](../data-plane/FINDINGS_FROM_THE_CONTROL_PLANE.md) in the data plane: per
>   copy, written by the node, never summed there, because a dual-run gives a shard two primaries and the reader
>   has to define the total. The heap, memory, CPU and load columns of `_cat/nodes` are `x` too: they are
>   per-node runtime that the tenant-visible projection omits on purpose.

### What changed on the data plane

A data node answers `cluster.health` and `cluster.state` from its own fabricated cluster state. That describes one
node's part of the fleet, and on a node shared by several tenants, it covers every tenant's indices on that node.
So a tenant asking for the cluster's health got another tenant's index names and health in the reply.

As of the commit that sends this, **a request carrying `x-tenant-id` is refused** with `400` and a reason saying the
control plane answers it (`ClusterManagerActionFilter`). That is the same treatment as a mutation. Without the header,
the node still answers with its own local view, which is what the platform team needs when debugging a node.

The refusal also covers anything answered by calling those two actions internally. `_cat/health`, `_cat/indices`,
`_cat/shards` and `_cat/nodes` all read `cluster.health` or `cluster.state`, so they are refused for a tenant too.
**The auth proxy has to route these to the control plane.** A misrouted one fails visibly, rather than answering with
one node's partial view.

### Why here and not on the data plane

The answer is a read of documents the supervisor already writes. The user's preference is that anything which can be
kept out of the data plane should be, since a fault in a stateless control plane costs nothing a node is serving. The
README already lists both operations as stubbed, waiting for these projections.

### What the projections give you, and the one trap

- `tenant/<t>/status/cluster`: status, node counts, and active, unassigned and initializing shard counts.
- `tenant/<t>/status/indices/<uuid>`: per-index status and counts, and every observed copy, with role and state.
- `tenant/<t>/status/nodes/<node-id>`: the nodes serving this tenant, and only those, by design (see
  `SUPERVISOR_KEYS.md`).

**Build health from the supervisor's figures, not by recomputing from a routing table.** The index projection lists
only *observed* copies. A shard missing a replica has no unassigned entry for it, so OpenSearch's own health
calculation over a routing table built from the projection would report green where the supervisor correctly says
yellow. The supervisor computed status and counts against what was asked for, so serve those.

**Use the tenant's own index names.** The data plane fabricates names like `default.my-index.289b099732`. The
projection's `index` field is the name the tenant knows, and that is the one to answer with.

`level=indices` and `level=shards` compose from the per-index documents. `status/cluster` deliberately carries no
per-index map.

### What a change should be careful about

- **`wait_for_status`, `wait_for_nodes` and the like.** Some clients and test harnesses send
  `wait_for_status=yellow&timeout=30s` and expect the request to wait. Polling the projections until the timeout keeps
  that working. Answering at once, with `timed_out: true`, does not.
- **Staleness.** `observed_at` and `observed_pass` say how old a projection is. A projection that has stopped updating
  should be visible, not served as current.
- **`_cat/indices` and `_cat/shards`** also show document counts and store sizes. The projections carry neither, though
  each copy's `size_bytes` is in the node inventories.

### Tests worth having

- a shard with a missing replica reports yellow, from the projection, not green;
- `level=indices` names the tenant's indices by their own names, and only that tenant's;
- `cluster.state`'s `nodes` lists only the nodes in `status/nodes` for that tenant.

---

## 2. An index's created version has to be fixed when it is created, and this component has the honest value

> **Done** as recommended, apart from resize and restore, which are not implemented here.
>
> - **The capability.** `minimum_node_version: "3.10.0"` in `capabilities.yaml`, the oldest version among the data
>   nodes, read as `DataPlaneCapabilities.minimumNodeVersion()`. `tools/dump-capabilities.sh` now takes any number
>   of node URLs and takes the minimum over every data node they report, because, as you say, one node's
>   `_nodes` gives only that node; it fails rather than skip a node it cannot reach. With no minimum declared,
>   nothing is stamped.
> - **The field.** `"version_created": "3.10.0"`, top-level beside `uuid` and `creation_date`, as you suggested,
>   written at create and on a data stream's backing index, and never again: `put_settings` and everything else
>   leave it. Rendered as `index.version.created` in vanilla's encoding (`137317827` for 3.10.0) in
>   `indices.get_settings`, `indices.get` and `cluster.state`.
> - **Refused from a client**, with vanilla's create, template and update messages, whether or not a settings
>   registry is loaded. Without a registry nothing else is checked, so `index.uuid`, `index.creation_date` and
>   `index.provided_name`, the other settings held top-level, are now refused that way too.
> - **The README's divergence row is gone**; the README has a section on the minimum version instead, with the
>   downgrade caveat for operators.
>
> Tests: `VersionCreatedTests`, `NodeVersionTests`, and the capability loading in `DataPlaneCapabilitiesTests`,
> covering each case in the list below. Your half can read `version_created` from the index document now.

### What happens

The data plane has no created version to use, so it stamps `index.version.created = Version.CURRENT` on every pass
(`DataNodeState.java:657`, and `CoordinatorState.java:668` for a coordinator's metadata). The value is whatever each
node runs, and it changes when a node is upgraded. Vanilla sets it once, at creation, to
`min(Version.CURRENT, nodes.getSmallestNonClientNodeVersion())` (`MetadataCreateIndexService.java:1121-1124`), and
nothing changes it after that.

This component leaves it out on purpose: `CreateIndexHandler` and `IndexStore.reportedSettings` say it "has no honest
value for" it, and the README lists it under *Considered divergences*. With the minimum data-node version as a
capability, there is now an honest value. A PR reviewer asked for exactly this consistency, suggesting the supervisor;
this component is the better owner, below.

### Why it is wrong

Read in OpenSearch at `3aa411be` and Lucene at `5b05d602`; nothing here has been run. About 110 places read the value.
The ones that matter:

- **Gates on what Lucene writes.** For an index with an index sort, a created version `>= 3.2.0` makes the
  `IndexWriter` configure a parent field (`NativeLuceneIndexWriterFactory.java:211`, `Store.java:310`). Lucene refuses
  that on an existing index that has none: "can't add a parent field to an already existing index without a parent
  field" (`IndexWriter.java:1124-1129`), so the writer does not open. A created version `>= 3.3.0` turns the
  doc-values skip index on by default for the sort field and `@timestamp` (`DateFieldMapper.java:912`), and Lucene
  refuses to change a field's skip-index type in an existing index (`FieldInfo.java:312-322`), so writes of that field
  fail.
- **When the per-node value hits them:** the version that opens a writer differs from the one that wrote the files.
  That happens when a node is upgraded past such a gate and restarts with its data; when a segrep replica on a newer
  node is promoted, or a pull-based primary is placed on one; and when a docrep replica recovers files from a primary
  on another version. Neither gate bites at 3.10, which is past both. But two were added in two releases, so any
  upgrade may cross a new one, and existing indices then fail on the upgraded nodes.
- **Behaviour that differs between copies while versions are mixed.** Analysis (`AnalysisRegistry.java:127, 593`,
  the `>= 3.0` checks in `CommonAnalysisModulePlugin`), similarity (`SimilarityService.java:147`) and mapper defaults
  key on the value. Copies on different versions can analyse and score differently, and docrep copies each index the
  same documents themselves. Small, but it is a difference from vanilla.
- **Compatibility checks that cannot fire.** Vanilla keeps a node older than an index's created version out of the
  cluster (`JoinTaskExecutor.java:448`), and rejects an index older than the minimum compatible version
  (`MetadataIndexUpgradeService.java:136-161`). With `Version.CURRENT` both pass by construction.
- **Not a concern:** mapping strictness takes the version but ignores it (`checkNoRemainingFields`,
  `isDeprecatedParameter`); `IndexRequest.process` does not use it; ingestion status is serialised only for indices
  created `>= 3.3.0` (`IndexMetadata.java:2753`), which a value of at least 3.10 always is.

### Why here, and not in the supervisor

The created version is index configuration: chosen once at creation and never changed, like the uuid and creation
date this component already writes. Every node reads the index document, so a value there is consistent across all
shards and copies by construction, which is what the reviewer wants. The supervisor owns placement, not index
configuration, and putting it there would give one fact two owners.

### What the value should be

**The minimum data-node version, at the moment of creation**, which is vanilla's rule. This component is not tied to
an OpenSearch version, but it already learns what the data nodes can do from the data nodes. Make the minimum
data-node version one of those capabilities. Since this component is stateless and restarted often, its capabilities
can be assumed current.

### What a change must be careful about

- **The minimum, over every data node that could hold the index.** A capability read from one node gives that node's
  version, which during a rolling upgrade may be the newest. A stale minimum is safe: it is lower than the truth, and
  a newer node reads an older index. A minimum that is too high is not. An index stamped newer than some data node
  cannot be held there, and the data plane will decline it, as vanilla keeps such a node out. If the capability
  cannot say, write nothing and let the data plane fall back, rather than guess high.
- **Downgrades.** As in vanilla, once an index is stamped, rolling a data node back below that version leaves the node
  unable to hold it. Worth a line in the operator documentation.
- **Fixed at creation.** Never rewritten, and refused from a client as vanilla refuses it. On create, and so in a
  template's settings: "private index setting [index.version.created] can not be set explicitly"
  (`MetadataCreateIndexService.java:1785-1798`). On `put_settings`: "can not update private setting
  [index.version.created]; this setting is managed by OpenSearch" (`AbstractScopedSettings.java:643-646`).
- **Resize and restore keep the source's value.** A shrink, split or clone copies it from the source index
  (`MetadataCreateIndexService.java:2052`), and a restored index keeps the snapshot's. The setting is
  `UnmodifiableOnRestore`.
- **Shape in the index document.** Keep it top-level beside `uuid` and `creation_date`, held once and rendered back
  into `indices.get_settings`. Suggested: `"version_created": "3.10.0"`, a version string, since this component has no
  OpenSearch `Version` class. Render it in vanilla's encoding, which is what a client of `get_settings` sees: the id
  `major * 1000000 + minor * 10000 + revision * 100 + 99`, XOR `0x08000000`, as a string (`Version.java:292, 367`).
  For 3.10.0 that is `137317827`.
- **The data plane's half,** which this side will do once the field exists:
  - build with the document's value instead of `Version.CURRENT`;
  - decline a copy, with a reason, when the index was created after this node's version, or before its minimum
    compatible index version;
  - with no value in the document, keep today's `Version.CURRENT`, with a warning.

  It reads the field with the rest of `IndexConfig`, as it reads `creation_date`. The supervisor's projections
  need nothing.

### Recommendation

Add the minimum data-node version to the discovered capabilities, stamp it as `version_created` when an index is
created, copy it on resize and restore, and render it in `indices.get_settings`. Then remove the README's divergence
row. This side then reads it, and drops `Version.CURRENT` except as the fallback.

### Tests worth having

- creating an index records the capability's minimum version, and `indices.get_settings` shows it in vanilla's
  encoding (`137317827` for 3.10.0);
- with no minimum known, nothing is written;
- `index.version.created` in a create request or a template is refused with vanilla's create message, and in
  `put_settings` with its update message;
- a resize copies the source's value, and `put_settings` on another setting leaves it unchanged.
