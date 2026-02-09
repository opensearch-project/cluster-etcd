/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.cluster.etcd.changeapplier;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IngestionStatus;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.RecoverySource;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.UnassignedInfo;
import org.opensearch.common.compress.CompressedXContent;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.IndexService;
import org.opensearch.index.mapper.DocumentMapper;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.indices.IndicesService;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

public class DataNodeState extends NodeState {
    private static final Logger logger = LogManager.getLogger(DataNodeState.class);
    private static final String PAUSE_PULL_INGESTION_KEY = "pause_pull_ingestion";

    private final Map<String, IndexMetadataComponents> indices;
    private final Map<String, Set<DataNodeShard>> assignedShards;

    public DataNodeState(
        DiscoveryNode localNode,
        Map<String, IndexMetadataComponents> indices,
        Map<String, Set<DataNodeShard>> assignedShards
    ) {
        super(localNode);
        // The index metadata and shard assignment should be identical
        assert indices.keySet().equals(assignedShards.keySet());
        this.indices = indices;
        this.assignedShards = assignedShards;
    }

    /**
     * Determines the appropriate recovery source for a shard.
     * This prevents data loss on node restarts by using existing data when available.
     *
     * @param dataNodeShard the DataNodeShard containing all necessary information
     * @return the recovery source to use for this shard
     */
    private RecoverySource determineRecoverySource(DataNodeShard dataNodeShard) {
        String indexName = dataNodeShard.getIndexName();
        int shardNum = dataNodeShard.getShardNum();
        ShardRole role = dataNodeShard.getShardRole();

        logger.debug("Determining recovery source for shard {}[{}] with role {}", indexName, shardNum, role);

        // Regular replica shards will recover from primary
        if (role == ShardRole.REPLICA) {
            logger.info("Shard {}[{}] with role {} is replica, using PeerRecoverySource", indexName, shardNum, role);
            return RecoverySource.PeerRecoverySource.INSTANCE;
        }

        // For PRIMARY (and search replica fallthrough) shards: if previously allocated then existing, else empty
        String previousAllocationId = dataNodeShard.getAllocationId();
        if (previousAllocationId != null) {
            logger.info(
                "Shard {}[{}] with role {} was previously allocated (ID: {}), using ExistingStoreRecoverySource",
                indexName,
                shardNum,
                role,
                previousAllocationId
            );
            return RecoverySource.ExistingStoreRecoverySource.INSTANCE;
        } else {
            logger.info(
                "Shard {}[{}] with role {} was not previously allocated, using EmptyStoreRecoverySource",
                indexName,
                shardNum,
                role
            );
            return RecoverySource.EmptyStoreRecoverySource.INSTANCE;
        }
    }

    private static CompressedXContent canonicalMapping(Index index, Map<String, Object> newMapping, IndicesService indicesService) {
        try {
            IndexService indexService = indicesService.indexService(index);
            if (indexService == null) {
                Map<String, Object> topLevelMapping = new HashMap<>();
                topLevelMapping.put(MapperService.SINGLE_MAPPING_NAME, newMapping);
                XContentBuilder mappingBuilder = XContentFactory.jsonBuilder().map(topLevelMapping);
                return new CompressedXContent(BytesReference.bytes(mappingBuilder));
            }
            IndexMetadata indexMetadata = indexService.getMetadata();
            try (MapperService mapperService = indicesService.createIndexMapperService(indexMetadata)) {
                DocumentMapper existingDocumentMapper = mapperService.documentMapperParser()
                    .parse(MapperService.SINGLE_MAPPING_NAME, indexMetadata.mapping().source());
                DocumentMapper newDocumentMapper = mapperService.documentMapperParser()
                    .parse(MapperService.SINGLE_MAPPING_NAME, newMapping);
                DocumentMapper mergedDocumentMapper = existingDocumentMapper.merge(
                    newDocumentMapper.mapping(),
                    MapperService.MergeReason.MAPPING_UPDATE
                );
                return mergedDocumentMapper.mappingSource();
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to parse mapping for index " + index.getName(), e);
        }
    }

    @Override
    public ClusterState buildClusterState(ClusterState previousState, IndicesService indicesService) {
        ClusterState.Builder clusterStateBuilder = ClusterState.builder(ClusterState.EMPTY_STATE);
        clusterStateBuilder.version(previousState.version() + 1);
        DiscoveryNodes.Builder nodesBuilder = DiscoveryNodes.builder().localNodeId(localNode.getId()).add(localNode);

        clusterStateBuilder.nodes(nodesBuilder);
        RoutingTable.Builder routingTableBuilder = RoutingTable.builder();
        Metadata.Builder metadataBuilder = Metadata.builder();
        for (Map.Entry<String, IndexMetadataComponents> entry : indices.entrySet()) {
            Index index = new Index(entry.getKey(), entry.getKey());
            IndexMetadata oldIndexMetadata = previousState.metadata().index(index);

            IndexRoutingTable previousIndexRoutingTable = previousState.routingTable().index(index);
            IndexRoutingTable.Builder indexRoutingTableBuilder = IndexRoutingTable.builder(index);
            IndexMetadata.Builder indexMetadataBuilder = IndexMetadata.builder(entry.getKey());
            indexMetadataBuilder.putMapping(new MappingMetadata(canonicalMapping(index, entry.getValue().mappings(), indicesService)));
            Settings.Builder settingsBuilder = ClusterStateUtils.initializeSettingsBuilder(entry.getKey(), entry.getValue().settings());
            indexMetadataBuilder.settings(settingsBuilder);

            // Set ingestion status based on pause_pull_ingestion from additionalMetadata
            setIngestionStatus(indexMetadataBuilder, entry.getValue().additionalMetadata(), entry.getKey());

            if (oldIndexMetadata != null) {
                indexMetadataBuilder.version(oldIndexMetadata.getVersion())
                    .settingsVersion(oldIndexMetadata.getSettingsVersion())
                    .mappingVersion(oldIndexMetadata.getMappingVersion());
            }
            for (DataNodeShard dataNodeShard : assignedShards.get(index.getName())) {
                int shardNum = dataNodeShard.getShardNum();
                indexMetadataBuilder.primaryTerm(shardNum, 1);
                ShardRole role = dataNodeShard.getShardRole();
                ShardId shardId = new ShardId(index, shardNum);

                IndexShardRoutingTable.Builder newShardRoutingTable = new IndexShardRoutingTable.Builder(shardId);
                IndexShardRoutingTable previousShardRoutingTable = previousIndexRoutingTable == null
                    ? new IndexShardRoutingTable.Builder(shardId).build()
                    : previousIndexRoutingTable.shard(shardNum);

                UnassignedInfo unassignedInfo = new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "created");

                // Handle replica shards with primary allocation
                if (role == ShardRole.REPLICA && dataNodeShard.getPrimaryAllocation().isPresent()) {
                    DataNodeShard.ShardAllocation primaryAllocation = dataNodeShard.getPrimaryAllocation().get();
                    RemoteNode primaryNode = primaryAllocation.node();

                    if (previousShardRoutingTable.primaryShard() != null
                        && previousShardRoutingTable.primaryShard().currentNodeId().equals(primaryNode.nodeId())) {
                        newShardRoutingTable.addShard(previousShardRoutingTable.primaryShard());
                    } else {
                        ShardRouting primaryShardRouting = ShardRouting.newUnassigned(
                            shardId,
                            true,
                            false,
                            RecoverySource.ExistingStoreRecoverySource.INSTANCE,
                            unassignedInfo
                        )
                            .initialize(
                                primaryNode.nodeId(),
                                primaryAllocation.allocationId(),
                                ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE
                            )
                            .moveToStarted();
                        // Add the primary shard routing to the index routing table
                        newShardRoutingTable.addShard(primaryShardRouting);
                    }
                    nodesBuilder.add(primaryNode.toDiscoveryNode());
                }

                Set<String> inSyncAllocationIds = new HashSet<>();
                if (role == ShardRole.PRIMARY && dataNodeShard.getReplicaAssignments().isEmpty() == false) {
                    // If this is a primary, we need to add the replica shards
                    Map<String, DataNodeShard.ShardAllocation> replicaNodesMap = new HashMap<>(
                        dataNodeShard.getReplicaAssignments()
                            .stream()
                            .collect(Collectors.toMap(k -> k.node().nodeId(), Function.identity()))
                    );
                    for (DataNodeShard.ShardAllocation shardAllocation : replicaNodesMap.values()) {
                        RemoteNode replicaNode = shardAllocation.node();
                        ShardRouting replicaShardRouting = ShardRouting.newUnassigned(
                            shardId,
                            false,
                            false,
                            RecoverySource.PeerRecoverySource.INSTANCE,
                            unassignedInfo
                        ).initialize(replicaNode.nodeId(), shardAllocation.allocationId(), ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE);
                        if (shardAllocation.shardState() == DataNodeShard.ShardState.STARTED) {
                            inSyncAllocationIds.add(shardAllocation.allocationId());
                            replicaShardRouting = replicaShardRouting.moveToStarted();
                        }
                        // Add the replica shard routing to the index routing table
                        newShardRoutingTable.addShard(replicaShardRouting);
                        nodesBuilder.add(replicaNode.toDiscoveryNode());
                    }
                }

                Optional<ShardRouting> previouslyStartedShard = previousShardRoutingTable == null
                    ? Optional.empty()
                    : previousShardRoutingTable.shards()
                        .stream()
                        .filter(sr -> localNode.getId().equals(sr.currentNodeId()))
                        .filter(s -> s.started() || s.initializing())
                        .findAny();

                ShardRouting shardRouting;
                if (previouslyStartedShard.isPresent()) {
                    shardRouting = previouslyStartedShard.get();
                    logger.debug(
                        "Reusing existing ShardRouting for shard {}[{}] with allocation ID {}",
                        entry.getKey(),
                        shardNum,
                        shardRouting.allocationId().getId()
                    );
                } else {
                    // No previous shard in cluster state - use ETCD-based allocation ID preservation
                    RecoverySource recoverySource = determineRecoverySource(dataNodeShard);
                    shardRouting = ShardRouting.newUnassigned(
                        shardId,
                        role == ShardRole.PRIMARY,
                        role == ShardRole.SEARCH_REPLICA,
                        recoverySource,
                        unassignedInfo
                    );

                    // Determine recovery source for this shard
                    String previousAllocationId = dataNodeShard.getAllocationId();
                    shardRouting = shardRouting.initialize(
                        localNode.getId(),
                        previousAllocationId,
                        ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE
                    );

                    // ALLOCATION ID TRACKING: Log allocation ID changes for monitoring purposes
                    String currentAllocationId = shardRouting.allocationId().getId();

                    if (previousAllocationId != null) {
                        if (previousAllocationId.equals(currentAllocationId)) {
                            logger.info(
                                "✅ ALLOCATION ID PRESERVED: shard {}[{}] kept allocation ID {}",
                                entry.getKey(),
                                shardNum,
                                previousAllocationId
                            );
                        } else {
                            logger.info(
                                "🔄 ALLOCATION ID CHANGED: shard {}[{}] from {} to {}",
                                entry.getKey(),
                                shardNum,
                                previousAllocationId,
                                currentAllocationId
                            );
                        }
                    } else {
                        logger.debug(
                            "No previous allocation ID found for shard {}[{}], using new ID: {}",
                            entry.getKey(),
                            shardNum,
                            currentAllocationId
                        );
                    }
                }
                newShardRoutingTable.addShard(shardRouting);

                indexRoutingTableBuilder.addIndexShard(newShardRoutingTable.build());
                inSyncAllocationIds.add(shardRouting.allocationId().getId());
                indexMetadataBuilder.putInSyncAllocationIds(shardNum, inSyncAllocationIds);
                if (role == ShardRole.SEARCH_REPLICA) {
                    // For local search replicas, we have no reference to the primary shard, so we must claim that
                    // the index is search-only. Otherwise, an assertion in the RoutingNodes constructor will fail.
                    settingsBuilder.put(IndexMetadata.INDEX_BLOCKS_SEARCH_ONLY_SETTING.getKey(), true);
                }
                // Set settings again, in case we modified it above
                indexMetadataBuilder.settings(settingsBuilder);
            }
            IndexMetadata newIndexMetadata = indexMetadataBuilder.build();
            if (oldIndexMetadata != null && oldIndexMetadata.equals(newIndexMetadata) == false) {
                logger.info("Index metadata for index {} changed", index.getName());
                indexMetadataBuilder.version(oldIndexMetadata.getVersion() + 1);
                if (oldIndexMetadata.getSettings().equals(newIndexMetadata.getSettings()) == false) {
                    indexMetadataBuilder.settingsVersion(oldIndexMetadata.getSettingsVersion() + 1);
                }
                if (Objects.equals(oldIndexMetadata.mapping(), newIndexMetadata.mapping()) == false) {
                    indexMetadataBuilder.mappingVersion(oldIndexMetadata.getMappingVersion() + 1);
                }
                newIndexMetadata = indexMetadataBuilder.build();
            }

            routingTableBuilder.add(indexRoutingTableBuilder);
            metadataBuilder.put(newIndexMetadata, false);
        }
        clusterStateBuilder.routingTable(routingTableBuilder.build());
        clusterStateBuilder.metadata(metadataBuilder);

        return clusterStateBuilder.build();
    }

    /**
     * Sets the ingestion status on the index metadata builder based on the pause_pull_ingestion value
     * from additional metadata.
     */
    private static void setIngestionStatus(
        IndexMetadata.Builder indexMetadataBuilder,
        Map<String, Object> additionalMetadata,
        String indexName
    ) {
        if (additionalMetadata == null) {
            return;
        }

        Object pausePullIngestionValue = additionalMetadata.get(PAUSE_PULL_INGESTION_KEY);
        if (pausePullIngestionValue != null) {
            boolean pausePullIngestion = pausePullIngestionValue instanceof Boolean
                ? (Boolean) pausePullIngestionValue
                : Boolean.parseBoolean(String.valueOf(pausePullIngestionValue));
            indexMetadataBuilder.ingestionStatus(new IngestionStatus(pausePullIngestion));
            logger.debug("Set ingestion status for index {}: pause_pull_ingestion={}", indexName, pausePullIngestion);
        }
    }
}
