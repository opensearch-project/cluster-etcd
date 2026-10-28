/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.handler.indices;

import org.opensearch.controlplane.handler.OperationContext;
import org.opensearch.controlplane.handler.OperationHandler;
import org.opensearch.controlplane.resolver.IndexAbstractions;
import org.opensearch.controlplane.resolver.IndexNameExpressionResolver;
import org.opensearch.controlplane.resolver.IndexResolution;
import org.opensearch.controlplane.resolver.IndicesOptions;
import org.opensearch.controlplane.rest.RestRequest;
import org.opensearch.controlplane.rest.RestResponse;
import org.opensearch.controlplane.rest.RestStatus;

/**
 * {@code HEAD /{index}} — whether an index exists.
 *
 * <p>Status only, never a body: HEAD carries no payload, so a failure is a bare 404 rather than the
 * {@code index_not_found_exception} a GET or DELETE would return.
 *
 * <p>The expression is resolved rather than looked up directly, so aliases, comma-separated lists and
 * wildcards all work. The options are the ones a live cluster uses here, and the pairing is worth
 * noting because it is asymmetric: a wildcard matching nothing is a <b>200</b>
 * ({@code allow_no_indices}), while a concrete name that does not exist is a <b>404</b>
 * ({@code ignore_unavailable} off). So {@code HEAD /nomatch-*} succeeds and {@code HEAD /missing}
 * does not. Verified against OpenSearch 3.9.0.
 */
public final class ExistsIndexHandler implements OperationHandler {

    @Override
    public String operationGroup() {
        return "indices.exists";
    }

    @Override
    public RestResponse handle(RestRequest request, OperationContext context) {
        IndicesOptions options = IndicesOptions.forExists().withOverridesFrom(request);
        IndexResolution resolution = new IndexNameExpressionResolver(IndexAbstractions.from(context.store()))
            .resolve(request.pathParam("index"), options);

        // Status only, but the three outcomes are still distinct. Verified: HEAD /_foo is a 400, not a
        // 404 -- an expression that could never be an index name is a different failure from one that
        // simply is not there.
        RestStatus status = switch (resolution) {
            case IndexResolution.Resolved ignored -> RestStatus.OK;
            case IndexResolution.NotFound ignored -> RestStatus.NOT_FOUND;
            case IndexResolution.InvalidName ignored -> RestStatus.BAD_REQUEST;
        };
        return RestResponse.empty(status);
    }
}
