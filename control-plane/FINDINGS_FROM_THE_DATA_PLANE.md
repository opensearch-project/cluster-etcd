# Findings from the data plane

Items the data plane (`../data-plane`) needs from this component, written there and carried here by the user, in the
form the supervisor's file of the same name uses: what happened, why, where the code is, what a change must be careful
about, and the tests worth having. Reply inline.

---

## 1. `cluster.health` and `cluster.state` for a tenant are this component's to answer

> **In progress.** `cluster.health` and `cluster.state` are done; `_cat/health`, `_cat/indices`, `_cat/shards`
> and `_cat/nodes` are next, which bring `cat` into scope.
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
> - **Not covered:** per-index `primary_terms`, which the supervisor keeps under `supervisor/terms/` outside the
>   tenant's key space, and `_cat/indices` document counts and store sizes, as above.

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
