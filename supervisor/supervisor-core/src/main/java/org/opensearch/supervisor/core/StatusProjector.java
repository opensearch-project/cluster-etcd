package org.opensearch.supervisor.core;

import org.opensearch.controlplane.store.KeyValueStore;
import org.opensearch.supervisor.model.Json;
import org.opensearch.supervisor.model.Keys;
import org.opensearch.supervisor.model.NodeId;
import org.opensearch.supervisor.model.TenantId;
import org.opensearch.supervisor.model.observe.Observation;
import org.opensearch.supervisor.model.status.IndexStatus;
import org.opensearch.supervisor.model.status.NodeStatus;
import org.opensearch.supervisor.model.status.StatusComputation;

import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Writes the projections the control plane reads: per-index health and placement, the cluster summary, the
 * tenant-scoped node list, and the awareness values.
 *
 * <p>Each of these unblocks an operation that returns {@code 501} today — {@code cluster.health},
 * {@code cluster.state}, and the half of weighted-routing validation the control plane cannot do — so this is
 * the point at which the supervisor stops being only a reconciler and starts being the reason another service
 * can answer a question.
 *
 * <p><b>Written only when the content changes.</b> Rewriting a per-index document every pass for ten thousand
 * unchanged indices would make the supervisor the store's dominant writer, ahead of the nodes it watches.
 * Change is judged on the content minus its timestamps, for the reason {@link Projector} explains: a document
 * carrying the pass it was computed at would otherwise defeat the check by changing on its own.
 *
 * <p><b>Not written in read-only mode</b>, and that costs something worth naming. One document serves four
 * readers — health, cluster state, coordinator routing, and docrep peer resolution — which was the right call
 * for write amplification but means health cannot be published without also publishing routing that
 * coordinators will act on. So a read-only supervisor stays genuinely read-only and publishes none of it.
 * Splitting the routing half out would allow observation without behaviour change; it is not worth doing until
 * somebody wants it.
 */
public final class StatusProjector {

    private final KeyValueStore store;

    /** Digest of the content this process last wrote per key, so an unchanged document costs no write. */
    private final Map<String, String> lastWritten = new HashMap<>();

    private int written;

    public StatusProjector(KeyValueStore store) {
        this.store = Objects.requireNonNull(store, "store");
    }

    /** @return how many keys were actually written this pass */
    public int project(Observation observation) {
        return project(observation, java.util.List.of());
    }

    /**
     * @param awarenessAttributes the pool's declared awareness attributes, which decide what
     *     {@code status/awareness} may offer as a weighted-routing dimension
     * @return how many keys were actually written this pass
     */
    public int project(Observation observation, java.util.List<String> awarenessAttributes) {
        int before = written;

        for (TenantId tenant : observation.tenants().keySet()) {
            StatusComputation.TenantStatus status =
                StatusComputation.compute(tenant, observation, awarenessAttributes);

            Set<String> live = new LinkedHashSet<>();

            status.indices().forEach((uuid, index) -> {
                String key = Keys.indexStatus(tenant, uuid);
                live.add(key);
                write(key, index.toBytes());
            });

            status.nodes().forEach((nodeId, node) -> {
                String key = Keys.nodeStatus(tenant, nodeId);
                live.add(key);
                write(key, node.toBytes());
            });

            if (status.cluster() != null) {
                write(Keys.clusterStatus(tenant), status.cluster().toBytes());
            }
            if (status.awareness() != null) {
                write(Keys.awarenessStatus(tenant), status.awareness().toBytes());
            }

            // A projection for an index or node that has gone must be deleted, not left behind. The control
            // plane reads this subtree by prefix scan, so a stale document is not merely untidy -- it is an
            // index that answers cluster.health after it has been deleted.
            reap(Keys.indexStatusPrefix(tenant), live);
            reap(Keys.nodeStatusPrefix(tenant), live);
        }

        return written - before;
    }

    private void write(String key, byte[] bytes) {
        String digest = Json.digest(withoutTimestamps(bytes));
        if (digest.equals(lastWritten.get(key))) {
            return;
        }
        store.put(key, bytes);
        lastWritten.put(key, digest);
        written++;
    }

    /**
     * The content of a projection, ignoring when it was computed.
     *
     * <p>Crude on purpose: the two timestamp fields are stripped textually rather than by re-serialising a
     * modified object, because the alternative is a second parallel representation of every document that has
     * to be kept in step with the real one. If the field names change, the change check silently starts
     * rewriting every pass — which the write-count test would catch.
     */
    private static byte[] withoutTimestamps(byte[] bytes) {
        String json = new String(bytes, java.nio.charset.StandardCharsets.UTF_8);
        return json.replaceAll("\"observed_at\":[0-9]+,?", "")
            .replaceAll("\"observed_pass\":[0-9]+,?", "")
            .getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }

    private void reap(String prefix, Set<String> live) {
        for (String key : store.scanPrefix(prefix).keySet()) {
            if (live.contains(key) == false) {
                store.delete(key);
                lastWritten.remove(key);
            }
        }
    }

    /** For tests and for the convergence document. */
    public int keysWritten() {
        return written;
    }

    /** Convenience for reading a projection back, which the control plane would do through its own store. */
    public static IndexStatus readIndexStatus(KeyValueStore store, TenantId tenant, String indexUuid) {
        return store.get(Keys.indexStatus(tenant, indexUuid))
            .map(versioned -> IndexStatus.from(versioned.value()))
            .orElse(null);
    }

    public static NodeStatus readNodeStatus(KeyValueStore store, TenantId tenant, NodeId node) {
        return store.get(Keys.nodeStatus(tenant, node))
            .map(versioned -> NodeStatus.from(versioned.value()))
            .orElse(null);
    }
}
