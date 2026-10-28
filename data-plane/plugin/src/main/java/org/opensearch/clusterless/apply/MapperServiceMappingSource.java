/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.clusterless.apply;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.common.compress.CompressedXContent;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.index.Index;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.index.IndexService;
import org.opensearch.index.mapper.DocumentMapper;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.indices.IndicesService;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Mappings put through the mapper service, so that identical intent produces identical metadata.
 *
 * <p>Adapted from {@code cluster-etcd}, which found the need for it. The mapping in an index document is
 * whatever the control plane stored — the client's shape, normalised as far as the control plane normalises
 * things. What the server compares to decide whether a mapping changed is the parsed, merged, canonical form,
 * and those two are not the same string. Publish the raw form and every pass looks like a mapping update:
 * {@code mappingVersion} climbs for ever and the mapper service reparses on each one.
 *
 * <p>So an index that already exists is merged into rather than replaced: the existing mapper parses both
 * sides, merges them, and the merged mapping source is what goes into the metadata. An index that does not
 * exist yet has nothing to merge with, and falls back to the mapping as written — which is right, because the
 * first application is the one that creates it.
 *
 * <p>Needs {@code IndicesService}, which a plugin can only reach through Guice, hence the supplier. A null one
 * is ordinary rather than exceptional: nothing is applied at this milestone, so nothing has an
 * {@code IndexService} yet, and the fallback is the only path that runs.
 */
public final class MapperServiceMappingSource implements MappingSource {

    private static final Logger logger = LogManager.getLogger(MapperServiceMappingSource.class);

    private final Supplier<IndicesService> indicesService;

    public MapperServiceMappingSource(Supplier<IndicesService> indicesService) {
        this.indicesService = indicesService;
    }

    @Override
    public MappingMetadata mappingFor(Index index, Map<String, Object> mapping) {
        if (mapping == null || mapping.isEmpty()) {
            return null;
        }
        IndicesService indices = indicesService.get();
        IndexService indexService = indices == null ? null : indices.indexService(index);
        if (indexService == null || indexService.getMetadata().mapping() == null) {
            // Nothing to merge with, so the written form *is* the canonical one. Two ways to get here, and the
            // second was a crash: no index yet, which is the ordinary first application; or an index created with
            // no mappings at all, which is what the control plane produces for `indices.create` with no body.
            // Setting a mapping on such an index then reached the merge path with a null existing mapping and
            // threw an NPE inside the applier -- and cluster-etcd's version of this method has the same latent
            // bug, since it was copied from there faithfully enough to include it.
            return MappingSource.WRAPPED.mappingFor(index, mapping);
        }
        try {
            return new MappingMetadata(merged(indexService, indices, mapping));
        } catch (Exception e) {
            // A mapping that will not merge is a real problem, but it is this index's problem. Falling back to
            // the written form keeps the other indices in the pass buildable, and the difference shows up as a
            // mapping version that moves when it should not rather than as a lost cluster state.
            logger.warn("clusterless: could not canonicalise the mapping for [{}]; using it as written", index.getName(), e);
            return MappingSource.WRAPPED.mappingFor(index, mapping);
        }
    }

    private static CompressedXContent merged(IndexService indexService, IndicesService indices, Map<String, Object> mapping)
        throws Exception {
        IndexMetadata existing = indexService.getMetadata();
        try (MapperService mapperService = indices.createIndexMapperService(existing)) {
            DocumentMapper existingMapper = mapperService.documentMapperParser()
                .parse(MapperService.SINGLE_MAPPING_NAME, existing.mapping().source());
            DocumentMapper incoming = mapperService.documentMapperParser().parse(MapperService.SINGLE_MAPPING_NAME, wrap(mapping));
            return existingMapper.merge(incoming.mapping(), MapperService.MergeReason.MAPPING_UPDATE).mappingSource();
        }
    }

    private static CompressedXContent wrap(Map<String, Object> mapping) throws Exception {
        Map<String, Object> wrapped = new HashMap<>();
        wrapped.put(MapperService.SINGLE_MAPPING_NAME, mapping);
        try (XContentBuilder builder = XContentFactory.jsonBuilder().map(wrapped)) {
            return new CompressedXContent(BytesReference.bytes(builder));
        }
    }
}
