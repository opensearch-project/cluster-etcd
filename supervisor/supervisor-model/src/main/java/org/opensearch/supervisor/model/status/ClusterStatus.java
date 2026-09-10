package org.opensearch.supervisor.model.status;

import org.opensearch.supervisor.model.Json;

import java.util.Map;

/**
 * {@code tenant/<t>/status/cluster} — what {@code cluster.health} answers, minus the per-index detail.
 *
 * <p><b>Deliberately no per-index map.</b> {@code cluster.health?level=indices} composes that from the
 * per-index documents, which keeps this one small and fixed-size so it can be rewritten every pass. Putting a
 * map of every index in the document that changes most often is the same mistake the node heartbeat avoids.
 *
 * @param indicesByStatus how many indices are green, yellow and red — three numbers rather than a list, for
 *     the same reason
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
    long observedPass
) {

    public ClusterStatus {
        indicesByStatus = indicesByStatus == null ? Map.of() : Map.copyOf(indicesByStatus);
    }

    public byte[] toBytes() {
        return Json.write(this, "cluster status");
    }

    public static ClusterStatus from(byte[] bytes) {
        return Json.read(bytes, ClusterStatus.class, "cluster status");
    }
}
