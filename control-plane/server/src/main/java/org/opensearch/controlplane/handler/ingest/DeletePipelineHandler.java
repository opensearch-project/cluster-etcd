/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.handler.ingest;

import org.opensearch.controlplane.handler.OperationHandler;
import org.opensearch.controlplane.rest.ErrorResponses;
import org.opensearch.controlplane.rest.Json;
import org.opensearch.controlplane.rest.RestRequest;
import org.opensearch.controlplane.rest.RestResponse;
import org.opensearch.controlplane.handler.OperationContext;

/**
 * {@code DELETE /_ingest/pipeline/{id}} — deletes one or more pipeline definitions.
 *
 * <p>The not-found rules are less obvious than they look. All three branches below were verified
 * against OpenSearch 3.9.0-SNAPSHOT and mirror {@code IngestService.innerDelete}:
 *
 * <ol>
 *   <li><b>No ingest metadata has ever existed</b> → {@code 200}, whatever the ID. The server
 *       returns the cluster state unchanged before it ever looks for a match. This is why deleting a
 *       nonexistent pipeline on a pristine cluster succeeds, but the same call after any pipeline has
 *       ever been created fails.</li>
 *   <li><b>Nothing matched and the ID is exactly {@code *}</b> → {@code 200}. Match-all is exempt
 *       from the not-found check, so "delete all pipelines" is a no-op rather than an error.</li>
 *   <li><b>Nothing matched, any other expression</b> → {@code 404 resource_not_found_exception}
 *       with reason {@code pipeline [<expression>] is missing}. This includes partial wildcards:
 *       {@code logs-*} matching nothing is a 404, even though {@code *} is not.</li>
 * </ol>
 *
 * <p>The spec declares only a 200 for this operation, so none of this is documented.
 */
public final class DeletePipelineHandler implements OperationHandler {

    /** OpenSearch exempts only the exact match-all pattern, not wildcards generally. */
    private static final String MATCH_ALL = "*";


    @Override
    public String operationGroup() {
        return "ingest.delete_pipeline";
    }

    @Override
    public RestResponse handle(RestRequest request, OperationContext context) {
        String idExpression = request.pathParam("id");
        PipelineStore pipelines = new PipelineStore(context.store());

        // Branch 1: no ingest metadata at all, so there is nothing to look in.
        if (pipelines.hasIngestMetadata() == false) {
            return Json.acknowledged(request);
        }

        var matched = pipelines.resolve(idExpression);
        if (matched.isEmpty() && MATCH_ALL.equals(idExpression) == false) {
            // Branch 3.
            return ErrorResponses.resourceNotFound(request, "pipeline [" + idExpression + "] is missing");
        }

        // Branch 2 falls through to here with nothing to remove, which is the intended no-op.
        matched.keySet().forEach(pipelines::delete);
        return Json.acknowledged(request);
    }
}
