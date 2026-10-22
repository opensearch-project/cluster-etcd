/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.clusterless.apply;

import org.opensearch.cluster.routing.AllocationId;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.ShardRoutingState;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.io.UncheckedIOException;

/**
 * A primary's routing entry relocating to another node, under allocation ids someone else chose.
 *
 * <p>A relocation is one entry, the source's, and the target's entry is derived from it, so the target's node has to
 * build the source's entry exactly as the source's node did. The server offers no public way to: {@code relocate} mints
 * the target's id, {@code AllocationId}'s constructors are private, and so are {@code ShardRouting}'s, apart from
 * {@code protected} ones. So the id comes from {@code AllocationId.fromXContent}, which is public, and the entry from a
 * subclass of {@code ShardRouting} that reaches the protected constructor. The subclass is then written out and read
 * back, because a cluster state holding a subclass would compare unequal to the same entry from the server
 * ({@code ShardRouting.equals} compares classes) once the server rebuilds it, as {@code LocalShardStateAction} does.
 */
final class RelocatingEntry {

    private RelocatingEntry() {}

    /** The started primary on {@code sourceNode}, relocating to {@code targetNode}, under the two ids given. */
    static ShardRouting of(ShardId shardId, String sourceNode, String targetNode, String sourceAllocationId, String targetAllocationId) {
        Built built = new Built(
            shardId,
            sourceNode,
            targetNode,
            allocationId(sourceAllocationId, targetAllocationId)
        );
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            built.writeTo(out);
            return new ShardRouting(out.bytes().streamInput());
        } catch (IOException e) {
            throw new UncheckedIOException("could not build the relocating entry for " + shardId, e);
        }
    }

    private static AllocationId allocationId(String id, String relocationId) {
        String json = "{\"id\":\"" + id + "\",\"relocation_id\":\"" + relocationId + "\"}";
        try (
            XContentParser parser = JsonXContent.jsonXContent.createParser(
                NamedXContentRegistry.EMPTY,
                DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                json
            )
        ) {
            return AllocationId.fromXContent(parser);
        } catch (IOException e) {
            throw new UncheckedIOException("could not build allocation id [" + id + "] relocating as [" + relocationId + "]", e);
        }
    }

    private static final class Built extends ShardRouting {
        Built(ShardId shardId, String sourceNode, String targetNode, AllocationId allocationId) {
            super(
                shardId,
                sourceNode,
                targetNode,
                true,
                false,
                ShardRoutingState.RELOCATING,
                null,
                null,
                allocationId,
                ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE
            );
        }
    }
}
