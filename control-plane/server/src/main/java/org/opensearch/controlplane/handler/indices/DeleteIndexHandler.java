/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.handler.indices;

import org.opensearch.controlplane.handler.OperationContext;
import org.opensearch.controlplane.handler.OperationHandler;
import org.opensearch.controlplane.rest.ErrorResponses;
import org.opensearch.controlplane.rest.Json;
import org.opensearch.controlplane.rest.RestRequest;
import org.opensearch.controlplane.rest.RestResponse;

/**
 * {@code DELETE /{index}} — deletes an index.
 *
 * <p>Implemented alongside create because a create you cannot undo leaves the service unusable, and
 * because the conformance stories delete what they create in their epilogues.
 *
 * <p>Removes the metadata and records a task so the shard manager can tear down the shards. The
 * metadata goes first: if the task write then fails, the index is gone from the control plane's point
 * of view and the shards are orphaned, which a reconciliation sweep can detect. The reverse order
 * would leave a task referring to metadata that still exists, which looks like a live index.
 *
 * <p>Does not yet resolve wildcards or comma-separated lists, both of which a real cluster accepts.
 */
public final class DeleteIndexHandler implements OperationHandler {

    @Override
    public String operationGroup() {
        return "indices.delete";
    }

    @Override
    public RestResponse handle(RestRequest request, OperationContext context) {
        String index = request.pathParam("index");
        IndexStore indices = new IndexStore(context.store());

        String uuid = indices.get(index).map(metadata -> metadata.path("uuid").asText("_na_")).orElse(null);
        if (uuid == null || indices.delete(index) == false) {
            return ErrorResponses.indexNotFound(request, index);
        }

        new IndexManagementTasks(context.store()).submit(
            IndexManagementTasks.TYPE_DELETE_INDEX,
            index,
            uuid,
            context.nowEpochMillis()
        );

        return Json.acknowledged(request);
    }
}
