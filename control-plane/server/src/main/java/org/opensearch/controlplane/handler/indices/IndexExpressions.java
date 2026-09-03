/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.handler.indices;

import org.opensearch.controlplane.resolver.IndexAbstractions;
import org.opensearch.controlplane.resolver.IndexNameExpressionResolver;
import org.opensearch.controlplane.resolver.IndexResolution;
import org.opensearch.controlplane.resolver.IndicesOptions;
import org.opensearch.controlplane.rest.ErrorResponses;
import org.opensearch.controlplane.rest.RestRequest;
import org.opensearch.controlplane.rest.RestResponse;
import org.opensearch.controlplane.store.KeyValueStore;

import java.util.List;

/**
 * Resolving the {@code {index}} placeholder, and rendering the two ways it can fail.
 *
 * <p>Every read in the {@code indices} namespace opens the same way — resolve the expression, and if it
 * could not be resolved answer either {@code index_not_found_exception} or
 * {@code invalid_index_name_exception}. Shared so that the mapping between a resolution failure and its
 * response shape is decided once: the two are reported differently by OpenSearch, and a handler that
 * collapsed them would be wrong in a way no test of that handler alone would catch.
 */
final class IndexExpressions {

    private IndexExpressions() {}

    /**
     * Either the concrete index names, or the response to send instead.
     *
     * <p>A result type rather than an exception, matching how {@link IndexResolution} already models
     * this: a missing index is an expected outcome of a request, not an exceptional condition.
     */
    record Resolved(List<String> indices, RestResponse failure) {

        boolean failed() {
            return failure != null;
        }
    }

    static Resolved resolve(RestRequest request, KeyValueStore store, IndicesOptions options) {
        IndexResolution resolution = new IndexNameExpressionResolver(IndexAbstractions.from(store))
            .resolve(request.pathParam("index"), options.withOverridesFrom(request));

        return switch (resolution) {
            case IndexResolution.Resolved resolved -> new Resolved(resolved.indices(), null);
            case IndexResolution.NotFound notFound ->
                new Resolved(List.of(), ErrorResponses.indexNotFound(request, notFound.expression()));
            case IndexResolution.InvalidName invalid -> new Resolved(
                List.of(),
                ErrorResponses.invalidIndexName(request, invalid.expression(), invalid.reason())
            );
        };
    }
}
