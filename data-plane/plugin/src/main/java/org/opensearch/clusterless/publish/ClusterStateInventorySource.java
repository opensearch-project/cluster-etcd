/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.clusterless.publish;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.routing.RoutingNode;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.clusterless.ClusterlessIndexSettings;
import org.opensearch.clusterless.contract.ShardInventory;
import org.opensearch.clusterless.contract.ShardRole;
import org.opensearch.clusterless.contract.ShardState;
import org.opensearch.clusterless.contract.TenantId;
import org.opensearch.core.index.shard.ShardId;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;

/**
 * The inventory, read from the cluster state this plugin itself fabricated.
 *
 * <p>Reading back what we wrote sounds circular and is not, because the two say different things. The
 * fabricated state is intent — "this node should hold these copies" — and the routing table's shard states
 * are the server's answer to it: a copy stays {@code INITIALIZING} until recovery finishes, and never
 * appears at all if the index could not be created. So this reports what the node <em>has</em>, which is
 * the whole point of the document. The alternative, reporting the assignment back, would make the
 * supervisor blind in a way nothing downstream could detect.
 *
 * <p>An index in the state without the identity settings is skipped rather than guessed at. It should not
 * happen — this plugin puts them there — but an index with no tenant cannot be named in the contract's
 * vocabulary at all, and inventing a tenant for it would attribute a shard to somebody.
 */
public final class ClusterStateInventorySource implements ShardInventorySource {

    private static final Logger logger = LogManager.getLogger(ClusterStateInventorySource.class);

    /**
     * How large a copy is, or {@code null} if that is not knowable yet.
     *
     * <p>A seam because the answer comes from {@code IndicesService}, which a plugin can only reach
     * through Guice, and because "not knowable yet" has to be expressible: {@code size_bytes} is
     * <b>omitted rather than zero</b> while a copy is initializing, since zero is a real size and would
     * tell a disk-aware decider the copy costs nothing.
     */
    @FunctionalInterface
    public interface ShardSizes {
        Long sizeOf(ShardId shardId);

        ShardSizes UNKNOWN = shardId -> null;
    }

    private final Supplier<ClusterState> state;
    private final String localNodeId;
    private final ShardSizes shardSizes;

    /** Indices already complained about, so a misconfigured one costs one log line rather than one per pass. */
    private final Set<String> warnedIndices = new HashSet<>();

    public ClusterStateInventorySource(Supplier<ClusterState> state, String localNodeId, ShardSizes shardSizes) {
        this.state = Objects.requireNonNull(state, "state");
        this.localNodeId = Objects.requireNonNull(localNodeId, "localNodeId");
        this.shardSizes = Objects.requireNonNull(shardSizes, "shardSizes");
    }

    @Override
    public List<ShardInventory.HeldCopy> heldCopies() {
        ClusterState current = state.get();
        RoutingNode routingNode = current.getRoutingNodes().node(localNodeId);
        if (routingNode == null) {
            // Before the first assignment is applied there is no routing node for this node at all. That
            // is "I hold nothing", which is a fact worth publishing, and not the same as "I could not
            // tell" — which would be an exception.
            return List.of();
        }

        List<ShardInventory.HeldCopy> copies = new ArrayList<>();
        for (ShardRouting routing : routingNode) {
            IndexMetadata indexMetadata = current.metadata().index(routing.index());
            if (indexMetadata == null) {
                // Routing without metadata is torn state, not corruption, and it costs a pass.
                logger.debug("clusterless: no metadata for {} yet; leaving it out of this inventory", routing.shardId());
                continue;
            }
            ShardInventory.HeldCopy copy = copyOf(routing, indexMetadata);
            if (copy != null) {
                copies.add(copy);
            }
        }
        return copies;
    }

    private ShardInventory.HeldCopy copyOf(ShardRouting routing, IndexMetadata indexMetadata) {
        String indexName = indexMetadata.getIndex().getName();
        String tenant = ClusterlessIndexSettings.TENANT.get(indexMetadata.getSettings());
        if (tenant == null || tenant.isEmpty()) {
            if (warnedIndices.add(indexName)) {
                logger.warn(
                    "clusterless: index [{}] carries no [{}], so its shards cannot be attributed to a tenant and are "
                        + "left out of this node's inventory",
                    indexName,
                    ClusterlessIndexSettings.TENANT.getKey()
                );
            }
            return null;
        }

        String tenantIndexName = ClusterlessIndexSettings.TENANT_INDEX_NAME.get(indexMetadata.getSettings());
        if (tenantIndexName == null || tenantIndexName.isEmpty()) {
            tenantIndexName = indexName;
        }

        ShardState shardState = ShardState.of(routing);
        // Omitted rather than zero while initializing, and only asked for when there is something to
        // measure — an initializing copy's store is a partial one, and its size would be a figure the
        // balancer would treat as final.
        Long sizeBytes = shardState == ShardState.STARTED ? shardSizes.sizeOf(routing.shardId()) : null;

        return new ShardInventory.HeldCopy(
            TenantId.of(tenant),
            indexMetadata.getIndexUUID(),
            tenantIndexName,
            ClusterlessIndexSettings.CONFIG_REVISION.get(indexMetadata.getSettings()),
            routing.shardId().id(),
            ShardRole.of(routing),
            shardState,
            sizeBytes,
            null
        );
    }
}
