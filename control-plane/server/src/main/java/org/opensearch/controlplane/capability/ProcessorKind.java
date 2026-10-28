/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.capability;

/**
 * The kinds of processor a pipeline can name, matching how a cluster reports them under
 * {@code GET /_nodes}.
 *
 * <p>They are separate namespaces, not one list: {@code sort} is a valid search response processor
 * and not a valid ingest processor, and a cluster with no plugins reports
 * {@code phase_results_processors: []} while still having plenty of the others.
 */
public enum ProcessorKind {

    /** {@code nodes.*.ingest.processors} */
    INGEST("ingest"),

    /** {@code nodes.*.search_pipelines.request_processors} */
    SEARCH_REQUEST("search_request"),

    /** {@code nodes.*.search_pipelines.response_processors} */
    SEARCH_RESPONSE("search_response"),

    /** {@code nodes.*.search_pipelines.phase_results_processors} */
    SEARCH_PHASE_RESULTS("search_phase_results");

    private final String configKey;

    ProcessorKind(String configKey) {
        this.configKey = configKey;
    }

    /** The key this kind appears under in a capabilities file. */
    public String configKey() {
        return configKey;
    }

    public static ProcessorKind fromConfigKey(String key) {
        for (ProcessorKind kind : values()) {
            if (kind.configKey.equals(key)) {
                return kind;
            }
        }
        throw new IllegalArgumentException(
            "unknown processor kind [" + key + "]; expected one of ingest, search_request, "
                + "search_response, search_phase_results"
        );
    }
}
