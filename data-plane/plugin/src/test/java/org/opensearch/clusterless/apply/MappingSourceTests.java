/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.clusterless.apply;

import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.core.index.Index;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Map;

/**
 * The mapping the fabricated metadata carries, including the case that crashed a node.
 *
 * <p>Only the fallback is testable here: the merging path needs an {@code IndicesService}, which needs a running
 * node. That is exactly where the crash was, so it is worth being precise about what these do and do not cover —
 * they pin the decision to *take* the fallback, and a running node is what proves the merge.
 */
public class MappingSourceTests extends OpenSearchTestCase {

    private static final Index INDEX = new Index("acme.my-index.abcdef0123", "uzKxLWZu4nPSrTknEcDpoA");

    public void testAMappingIsWrappedInTheSingleTypeTheServerStillHas() {
        // MappingMetadata derives its type from a single root key and refuses a map without one, so the wrapping
        // is not decoration.
        MappingMetadata mapping = MappingSource.WRAPPED.mappingFor(INDEX, Map.of("properties", Map.of("host", Map.of("type", "keyword"))));

        assertNotNull(mapping);
        assertEquals("_doc", mapping.type());
        assertTrue(mapping.source().string(), mapping.source().string().contains("\"_doc\""));
        assertTrue(mapping.source().string(), mapping.source().string().contains("keyword"));
    }

    public void testAnIndexWithNoMappingHasNoMappingMetadata() {
        // What `indices.create` with no body produces, and the state an index is in when the control plane later
        // sets a mapping on it. Returning an empty MappingMetadata instead would fabricate a mapping nobody wrote.
        assertNull(MappingSource.WRAPPED.mappingFor(INDEX, Map.of()));
        assertNull(MappingSource.WRAPPED.mappingFor(INDEX, null));
    }

    public void testTheMergingSourceFallsBackWhenThereIsNoIndicesService() {
        // Which is every pass until something is applied, and every pass in read-only mode.
        MappingSource source = new MapperServiceMappingSource(() -> null);

        MappingMetadata mapping = source.mappingFor(INDEX, Map.of("properties", Map.of("host", Map.of("type", "keyword"))));

        assertNotNull(mapping);
        assertEquals("_doc", mapping.type());
    }

    public void testTheMergingSourceStillReportsNoMappingAsNoMapping() {
        MappingSource source = new MapperServiceMappingSource(() -> null);
        assertNull(source.mappingFor(INDEX, Map.of()));
    }
}
