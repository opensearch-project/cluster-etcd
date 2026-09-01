/*
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.http;

import org.opensearch.controlplane.rest.RestResponse;

import java.util.List;
import java.util.Map;

/**
 * The seam between the HTTP transport and the routing layer, so the Netty front
 * end can be replaced without touching the router.
 */
public interface HttpRequestDispatcher {

    RestResponse dispatch(
        String method,
        String rawPath,
        String uri,
        Map<String, List<String>> queryParams,
        Map<String, String> headers,
        byte[] body
    );
}
