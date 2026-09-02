/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.handler.pipeline;

/**
 * The two flavours of pipeline, and the only things that differ between them.
 *
 * <p>Ingest pipelines and search pipelines are the same API twice over. Verified against OpenSearch
 * 3.9.0: identical response shapes, identical comma-separated and wildcard ID handling, identical
 * 404-with-{@code {}} when nothing matches, identical three-branch delete semantics down to the
 * metadata-presence quirk, and identical error wording — the search pipeline not-found message says
 * {@code pipeline [x] is missing}, not "search pipeline". That is not a coincidence: the search
 * pipeline implementation was derived from the ingest one.
 *
 * <p>So the handlers are written once and instantiated twice, rather than copied. A behaviour fixed
 * for one is fixed for both, which matters because these semantics took a live cluster to pin down
 * and would be easy to get subtly wrong on a second pass.
 *
 * @param keyPrefix          where definitions live, e.g. {@code ingest/pipeline/}
 * @param metadataMarkerKey  the "has ever had metadata" marker; see
 *                           {@link PipelineDefinitions#hasMetadata()}. Must sit outside
 *                           {@code keyPrefix} so it never appears in a definition scan.
 * @param requiredProperty   a body property the server insists on, or {@code null} if none. This is
 *                           the one genuine difference: ingest requires {@code processors}, while a
 *                           search pipeline may legitimately be {@code {}} because its three
 *                           processor lists are all optional.
 * @param putGroup           the {@code x-operation-group} for put
 * @param getGroup           the {@code x-operation-group} for get
 * @param deleteGroup        the {@code x-operation-group} for delete
 */
public record PipelineKind(
    String keyPrefix,
    String metadataMarkerKey,
    String requiredProperty,
    String putGroup,
    String getGroup,
    String deleteGroup
) {

    /** {@code /_ingest/pipeline} — requires {@code processors}, despite the spec not saying so. */
    public static PipelineKind ingest() {
        return new PipelineKind(
            "ingest/pipeline/",
            "ingest/metadata-initialized",
            "processors",
            "ingest.put_pipeline",
            "ingest.get_pipeline",
            "ingest.delete_pipeline"
        );
    }

    /** {@code /_search/pipeline} — no required property; {@code {}} is a valid definition. */
    public static PipelineKind search() {
        return new PipelineKind(
            "search/pipeline/",
            "search/metadata-initialized",
            null,
            "search_pipeline.put",
            "search_pipeline.get",
            "search_pipeline.delete"
        );
    }

    boolean hasRequiredProperty() {
        return requiredProperty != null;
    }
}
