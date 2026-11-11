/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.clusterless.apply;

import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.clusterless.contract.IndexConfig;
import org.opensearch.clusterless.contract.MalformedDocumentException;

/**
 * The {@code index.version.created} this node gives an index: one value per index, the same on every node.
 *
 * <p>OpenSearch fixes it at creation and never changes it, and it matters beyond bookkeeping. It gates what Lucene
 * writes: a parent field for an index-sorted index from 3.2.0, a doc-values skip index on dates from 3.3.0. Lucene
 * refuses either change on an existing index. So a per-node value, which this used to be ({@code Version.CURRENT}),
 * would make an upgraded node's writer fail on an index created before the next such gate. The control plane now
 * stamps the oldest data node's version into the index document, as vanilla uses the smallest node version.
 *
 * <p>In order: the version the index's applied metadata already carries, because the server refuses a change on a live
 * index ("version mismatch on settings update", {@code IndexSettings.updateIndexMetadata}); then the document's; then,
 * for a document the control plane did not stamp, this node's own, as before.
 */
final class CreatedVersion {

    private CreatedVersion() {}

    static Version of(IndexConfig config, IndexMetadata applied) {
        if (applied != null && Version.V_EMPTY.equals(applied.getCreationVersion()) == false) {
            return applied.getCreationVersion();
        }
        try {
            Version declared = declared(config);
            return declared == null ? Version.CURRENT : declared;
        } catch (MalformedDocumentException e) {
            // DesiredStateResolver declines a held index for this, with the reason. Reached by a coordinator's index,
            // which is only routed to, so it is not worth refusing.
            return Version.CURRENT;
        }
    }

    /**
     * The version the index document states, or null if it states none.
     *
     * @throws MalformedDocumentException if it states one this node cannot read
     */
    static Version declared(IndexConfig config) {
        if (config.versionCreated() == null) {
            return null;
        }
        try {
            return Version.fromString(config.versionCreated());
        } catch (IllegalArgumentException e) {
            throw new MalformedDocumentException(
                "index [" + config.name() + "]",
                "[" + IndexConfig.FIELD_VERSION_CREATED + "] is not a version: [" + config.versionCreated() + "]"
            );
        }
    }
}
