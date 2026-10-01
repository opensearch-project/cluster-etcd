/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.handler.core;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.opensearch.controlplane.handler.OperationContext;
import org.opensearch.controlplane.handler.OperationHandler;
import org.opensearch.controlplane.rest.Json;
import org.opensearch.controlplane.rest.RestRequest;
import org.opensearch.controlplane.rest.RestResponse;
import org.opensearch.controlplane.rest.RestStatus;

/**
 * {@code GET /} — cluster and version information.
 *
 * <p>Worth implementing before anything else looks urgent: every OpenSearch language client calls
 * this to negotiate features, and the spec repo's test harness will not run a single story until it
 * can read {@code version.number} from here. Until this exists, conformance testing is blocked
 * entirely.
 *
 * <p>Field order matches the server's, which is not required by JSON but makes a diff against a real
 * cluster readable.
 *
 * @see ClusterIdentity for what the reported values actually mean
 */
public final class InfoHandler implements OperationHandler {

    private final ClusterIdentity identity;

    public InfoHandler(ClusterIdentity identity) {
        this.identity = identity;
    }

    @Override
    public String operationGroup() {
        return "info";
    }

    @Override
    public RestResponse handle(RestRequest request, OperationContext context) {
        ObjectNode body = Json.object();
        body.put("name", identity.nodeName());
        body.put("cluster_name", identity.clusterName());
        body.put("cluster_uuid", ClusterUuid.resolve(context.store()));

        ObjectNode version = body.putObject("version");
        version.put("distribution", ClusterIdentity.DISTRIBUTION);
        version.put("number", identity.version());
        version.put("build_type", ClusterIdentity.BUILD_TYPE);
        version.put("build_hash", BuildInfo.hash());
        version.put("build_date", BuildInfo.date());
        version.put("build_snapshot", BuildInfo.isSnapshot());
        version.put("lucene_version", ClusterIdentity.LUCENE_VERSION);
        version.put("minimum_wire_compatibility_version", ClusterIdentity.MINIMUM_WIRE_COMPATIBILITY_VERSION);
        version.put("minimum_index_compatibility_version", ClusterIdentity.MINIMUM_INDEX_COMPATIBILITY_VERSION);

        body.put("tagline", ClusterIdentity.TAGLINE);

        return Json.response(RestStatus.OK, body, request);
    }
}
