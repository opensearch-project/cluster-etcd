/*
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.router;

import java.util.Set;

/**
 * One (method, path template) pair bound to an operation group.
 *
 * @param method          uppercase HTTP method
 * @param path            OpenAPI path template, e.g. {@code /{index}/_alias/{name}}
 * @param operationGroup  the OpenSearch {@code x-operation-group}, e.g. {@code indices.put_alias}
 * @param deprecated      whether the spec marks this particular route deprecated
 * @param queryParams     query parameters specific to this operation, excluding
 *                        {@link GeneratedRoutes#GLOBAL_QUERY_PARAMS}
 */
public record Route(
    String method,
    String path,
    String operationGroup,
    boolean deprecated,
    Set<String> queryParams
) {
    public Route {
        method = method.toUpperCase(java.util.Locale.ROOT);
        queryParams = Set.copyOf(queryParams);
    }

    @Override
    public String toString() {
        return method + " " + path;
    }
}
