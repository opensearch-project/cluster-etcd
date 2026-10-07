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

import java.time.Duration;
import java.time.Instant;
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
 * <p><b>Except {@code status/cluster}, which is also rewritten once {@link #DEFAULT_CLUSTER_HEARTBEAT} has passed</b>
 * (control plane finding 1). Written only on change, its {@code observed_at} stopped moving on a quiet tenant, so a
 * reader could not tell a healthy supervisor with nothing to say from no supervisor at all, and the control plane
 * answers {@code cluster.health} differently for the two. The heartbeat is published in the document as
 * {@code heartbeat_ms}. It is one small document per tenant, so the cost is one write per tenant per heartbeat; the
 * per-index and per-node documents gain nothing. Timed on the observation's clock, so it is at most one pass late.
 *
 * <p><b>{@code status/cluster} is written last</b>, after every other document of the tenant has been written or
 * removed, so a fresh one vouches for the whole pass. The control plane reads it as the tenant's freshness, so keep
 * it last. A pass that throws publishes nothing after the failure, so a pool whose passes keep failing goes stale,
 * which is the right answer.
 *
 * <p><b>Not written in read-only mode</b>, and that costs something worth naming. One document serves four
 * readers — health, cluster state, coordinator routing, and docrep peer resolution — which was the right call
 * for write amplification but means health cannot be published without also publishing routing that
 * coordinators will act on. So a read-only supervisor stays genuinely read-only and publishes none of it.
 * Splitting the routing half out would allow observation without behaviour change; it is not worth doing until
 * somebody wants it. A tenant under a read-only supervisor therefore reads as unmanaged to the control plane.
 */
public final class StatusProjector {

    /**
     * The longest {@code status/cluster} goes unwritten while this supervisor passes over the pool. A reader's
     * staleness threshold is this plus a failover (the lease TTL and a pass), so changing it moves theirs.
     */
    public static final Duration DEFAULT_CLUSTER_HEARTBEAT = Duration.ofSeconds(10);

    private final KeyValueStore store;
    private final Duration clusterHeartbeat;

    /** Digest of the content this process last wrote per key, so an unchanged document costs no write. */
    private final Map<String, String> lastWritten = new HashMap<>();

    /** When each tenant's {@code status/cluster} was last written, on the observation's clock. */
    private final Map<String, Instant> clusterWrittenAt = new HashMap<>();

    private int written;

    public StatusProjector(KeyValueStore store) {
        this(store, DEFAULT_CLUSTER_HEARTBEAT);
    }

    public StatusProjector(KeyValueStore store, Duration clusterHeartbeat) {
        this.store = Objects.requireNonNull(store, "store");
        this.clusterHeartbeat = Objects.requireNonNull(clusterHeartbeat, "clusterHeartbeat");
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

            if (status.awareness() != null) {
                write(Keys.awarenessStatus(tenant), status.awareness().toBytes());
            }

            // A projection for an index or node that has gone must be deleted, not left behind. The control
            // plane reads this subtree by prefix scan, so a stale document is not merely untidy -- it is an
            // index that answers cluster.health after it has been deleted.
            reap(Keys.indexStatusPrefix(tenant), live);
            reap(Keys.nodeStatusPrefix(tenant), live);

            // Last, so that a fresh one means everything above has landed. See the class comment.
            if (status.cluster() != null) {
                writeCluster(Keys.clusterStatus(tenant),
                    status.cluster().withHeartbeatMs(clusterHeartbeat.toMillis()).toBytes(), observation.observedAt());
            }
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

    /** As {@link #write}, and also once the heartbeat has passed, so its {@code observed_at} keeps moving. */
    private void writeCluster(String key, byte[] bytes, Instant observedAt) {
        Instant last = clusterWrittenAt.get(key);
        if (last != null && Duration.between(last, observedAt).compareTo(clusterHeartbeat) >= 0) {
            lastWritten.remove(key);
        }
        int before = written;
        write(key, bytes);
        if (written != before) {
            clusterWrittenAt.put(key, observedAt);
        }
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
