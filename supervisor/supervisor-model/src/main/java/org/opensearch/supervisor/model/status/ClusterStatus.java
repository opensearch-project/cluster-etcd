package org.opensearch.supervisor.model.status;

import org.opensearch.supervisor.model.Json;

import java.util.Map;

/**
 * {@code tenant/<t>/status/cluster} — what {@code cluster.health} answers, minus the per-index detail.
 *
 * <p><b>Deliberately no per-index map.</b> {@code cluster.health?level=indices} composes that from the
 * per-index documents, which keeps this one small and fixed-size, cheap enough to rewrite on a heartbeat. Putting a
 * map of every index in the document that changes most often is the same mistake the node heartbeat avoids.
 *
 * @param indicesByStatus how many indices are green, yellow and red — three numbers rather than a list, for
 *     the same reason
 * @param heartbeatMs the longest this document goes unwritten while a supervisor is passing over its pool, even when
 *     nothing in it has changed (control plane finding 1). A reader that sees {@code observed_at} older than this
 *     plus a failover is looking at a pool nothing is managing. Zero from a supervisor that predates the heartbeat.
 */
public record ClusterStatus(
    HealthStatus status,
    int numberOfNodes,
    int numberOfDataNodes,
    int activeShards,
    int activePrimaryShards,
    int unassignedShards,
    int initializingShards,
    Map<String, Integer> indicesByStatus,
    long observedAt,
    long observedPass,
    long heartbeatMs
) {

    public ClusterStatus {
        indicesByStatus = indicesByStatus == null ? Map.of() : Map.copyOf(indicesByStatus);
    }

    /** Before the heartbeat is known, which is how {@code StatusComputation} builds it; the projector stamps it. */
    public ClusterStatus(
        HealthStatus status,
        int numberOfNodes,
        int numberOfDataNodes,
        int activeShards,
        int activePrimaryShards,
        int unassignedShards,
        int initializingShards,
        Map<String, Integer> indicesByStatus,
        long observedAt,
        long observedPass
    ) {
        this(status, numberOfNodes, numberOfDataNodes, activeShards, activePrimaryShards, unassignedShards,
            initializingShards, indicesByStatus, observedAt, observedPass, 0L);
    }

    public ClusterStatus withHeartbeatMs(long heartbeat) {
        return new ClusterStatus(status, numberOfNodes, numberOfDataNodes, activeShards, activePrimaryShards,
            unassignedShards, initializingShards, indicesByStatus, observedAt, observedPass, heartbeat);
    }

    public byte[] toBytes() {
        return Json.write(this, "cluster status");
    }

    public static ClusterStatus from(byte[] bytes) {
        return Json.read(bytes, ClusterStatus.class, "cluster status");
    }
}
