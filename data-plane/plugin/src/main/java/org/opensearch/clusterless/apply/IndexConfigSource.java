/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.clusterless.apply;

import org.opensearch.clusterless.contract.IndexConfig;
import org.opensearch.clusterless.contract.TenantId;

import java.util.Optional;

/**
 * Where index settings and mappings come from.
 *
 * <p>A seam for one reason and it is not testability, although that follows: the resolver has to be able to
 * tell <b>absent</b> from <b>unreadable</b> from <b>present</b>, and those three lead to different behaviour.
 * An {@link Optional#empty()} means the document is not there — which is an ordinary, temporary state, since
 * the control plane writes it and the supervisor may have assigned the shard first. Unreadable throws.
 */
@FunctionalInterface
public interface IndexConfigSource {

    /**
     * @return the index document, or empty if there is no such key
     * @throws org.opensearch.clusterless.contract.MalformedDocumentException if there is one and it cannot be
     *     understood. That costs this index a pass, never the whole assignment.
     */
    Optional<IndexConfig> read(TenantId tenant, String indexName);
}
