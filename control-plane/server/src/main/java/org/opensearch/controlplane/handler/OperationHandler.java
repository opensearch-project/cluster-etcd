/*
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.handler;

import org.opensearch.controlplane.rest.RestRequest;
import org.opensearch.controlplane.rest.RestResponse;

/**
 * Implements one OpenSearch operation group.
 *
 * <p>One handler serves every route in its group. {@code indices.put_alias} is
 * eleven (method, path) pairs in the spec; it is one {@code OperationHandler}
 * here, and it reads whichever path placeholders were bound from
 * {@link RestRequest#pathParams()}.
 *
 * <p>Implementations are stateless singletons. Everything request-scoped — notably the
 * tenant-confined store — arrives in the {@link OperationContext}, so one instance
 * safely serves every tenant concurrently.
 */
public interface OperationHandler {

    /** The {@code x-operation-group} this handler serves, e.g. {@code indices.create}. */
    String operationGroup();

    RestResponse handle(RestRequest request, OperationContext context) throws Exception;
}
