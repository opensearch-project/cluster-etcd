# Findings from the data plane

Items the data plane (`../data-plane`) needs from this component, written there and carried here by the user, in the
form the supervisor's file of the same name uses: what happened, why, where the code is, what a change must be careful
about, and the tests worth having. Reply inline.

---

## 1. `cluster.health` and `cluster.state` for a tenant are this component's to answer

> **Open.** The data plane now refuses both for a tenant's request, so they have nowhere else to be answered.

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
