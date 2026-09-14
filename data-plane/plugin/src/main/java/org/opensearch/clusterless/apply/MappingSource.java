/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.clusterless.apply;

import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.core.index.Index;
import org.opensearch.index.mapper.MapperService;

import java.util.Map;

/**
 * Turns the mapping from an index document into the {@link MappingMetadata} a cluster state carries.
 *
 * <p>A seam because the good implementation needs {@code IndicesService}, which a plugin can only reach through
 * Guice, and because what it does matters more than it looks. The point of routing a mapping through the mapper
 * service is <b>canonicalisation</b>: byte-identical intent must produce byte-identical metadata, or every pass
 * sees a mapping change, bumps {@code mappingVersion}, and hands the server a new cluster state for no reason.
 * Two nodes holding copies of one shard must agree too.
 *
 * <p>{@link #WRAPPED} is the fallback and is honest about being one: it wraps the mapping under {@code _doc}
 * and serialises it as given. That is enough while nothing is applied, and enough for an index the node has not
 * created yet — which is exactly what {@code cluster-etcd} falls back to when {@code indexService} is null.
 */
@FunctionalInterface
public interface MappingSource {

    /**
     * @param mapping the {@code mappings} object from the index document, unwrapped — no {@code _doc} key
     * @return the metadata to put on the index, or {@code null} if the index has no mapping at all
     */
    MappingMetadata mappingFor(Index index, Map<String, Object> mapping);

    /**
     * The mapping as written, wrapped in the single type OpenSearch still has.
     *
     * <p>Wrapped rather than passed through, because {@code MappingMetadata} derives its type from a single
     * root key and refuses a map without one.
     */
    MappingSource WRAPPED = (index, mapping) -> mapping == null || mapping.isEmpty()
        ? null
        : new MappingMetadata(MapperService.SINGLE_MAPPING_NAME, Map.of(MapperService.SINGLE_MAPPING_NAME, mapping));
}
