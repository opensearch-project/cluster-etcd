/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.clusterless.apply;

import org.opensearch.clusterless.contract.IndexConfig;
import org.opensearch.clusterless.contract.IndexStatus;
import org.opensearch.clusterless.contract.NodeId;
import org.opensearch.clusterless.contract.NodeStatus;
import org.opensearch.clusterless.contract.TenantId;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Where every copy of a coordinated tenant's shards is, and how to reach the nodes holding them.
 *
 * <p>Read once per pass and handed to the fabrication already settled, for the reason everything else here is:
 * {@link CoordinatorState} is a pure function called twice on a pass that applies, and this is several reads of
 * the store.
 *
 * <p>Two projections joined with one index document each. The placement comes from
 * {@code tenant/<t>/status/indices/<uuid>}, the addresses from {@code tenant/<t>/status/nodes/<id>}, and the
 * settings and mappings from the control plane's {@code tenant/<t>/indices/<name>} — because a coordinator has to
 * put an index in its cluster state to route to it, and an index in a cluster state needs a shard count and a
 * mapping whether or not this node holds any of it.
 *
 * @param deferred indices and copies left out, with a reason each — the same discipline as
 *     {@link DesiredState#deferred()}, and for the same reason: a coordinator that silently cannot reach half a
 *     tenant's shards is indistinguishable from one whose tenant has half as many shards.
 */
public record CoordinatorView(List<RoutedIndex> indices, Map<NodeId, NodeStatus> peers, List<String> deferred) {

    public CoordinatorView {
        indices = indices == null ? List.of() : List.copyOf(indices);
        peers = peers == null ? Map.of() : Collections.unmodifiableMap(new java.util.LinkedHashMap<>(peers));
        deferred = deferred == null ? List.of() : List.copyOf(deferred);
    }

    /** Nothing coordinated, which is every node that was not given the role. */
    public static CoordinatorView nothing() {
        return new CoordinatorView(List.of(), Map.of(), List.of());
    }

    public boolean isEmpty() {
        return indices.isEmpty();
    }

    /**
     * One index this node routes for.
     *
     * @param config the control plane's document, for the settings and mappings the cluster state needs
     * @param placement the supervisor's observation of where the copies are, including ones not yet started
     */
    public record RoutedIndex(TenantId tenant, IndexConfig config, IndexStatus placement) {

        public RoutedIndex {
            Objects.requireNonNull(tenant, "tenant");
            Objects.requireNonNull(config, "config");
            Objects.requireNonNull(placement, "placement");
        }

        public String uuid() {
            return placement.uuid();
        }
    }
}
