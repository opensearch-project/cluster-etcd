/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.handler.pipeline;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.opensearch.controlplane.capability.GrokPatterns;
import org.opensearch.controlplane.handler.OperationContext;
import org.opensearch.controlplane.handler.OperationHandler;
import org.opensearch.controlplane.rest.Json;
import org.opensearch.controlplane.rest.RestRequest;
import org.opensearch.controlplane.rest.RestResponse;
import org.opensearch.controlplane.rest.RestStatus;

/**
 * {@code GET /_ingest/processor/grok} — the built-in grok pattern library.
 *
 * <p>Lives beside the pipeline handlers because it is in the {@code ingest} namespace, though it is not
 * about pipelines: it reports the pattern library a grok processor can refer to.
 *
 * <p>A pure passthrough of declared capability data, registered <b>only when the capabilities file
 * carries the patterns</b>; otherwise the operation keeps its 501, for the reason given on
 * {@code GetScriptLanguagesHandler}. Answering {@code {"patterns":{}}} would tell a client the cluster
 * has no grok patterns, which no real cluster does.
 *
 * <p>It is the one declared capability nothing validates against. A grok processor's {@code pattern} is
 * compiled by the node that runs it, and a pipeline may define its own patterns inline via
 * {@code pattern_definitions}, so there is nothing here to check a pipeline against — only something to
 * report.
 *
 * <h2>{@code ?s} and why the stored order is the unsorted one</h2>
 *
 * <p>{@code s} is a boolean, default {@code false}, and it is the whole reason
 * {@link GrokPatterns} keeps an ordered map:
 *
 * <ul>
 *   <li>default — {@code Grok.BUILTIN_PATTERNS}, a {@code LinkedHashMap} in pattern-file load order.
 *       Verified against 3.9.0: starts at {@code S3_REQUEST_LINE}.</li>
 *   <li>{@code ?s=true} — a {@code TreeMap} of the same map. Starts at {@code BACULA_CAPACITY}.</li>
 * </ul>
 *
 * <p>Sorting is recoverable from load order and load order is not recoverable from sorted, so the
 * capabilities file stores the former. The parameter name really is the single letter {@code s} — it is
 * not spelled {@code sort} or {@code sorted}, and the spec does not document it at all, which is worth
 * knowing before assuming a client passing {@code sort=true} will get sorted output. It will not, here or
 * on a real cluster.
 */
public final class GrokPatternsHandler implements OperationHandler {

    private final GrokPatterns patterns;

    public GrokPatternsHandler(GrokPatterns patterns) {
        this.patterns = patterns;
    }

    @Override
    public String operationGroup() {
        return "ingest.processor_grok";
    }

    @Override
    public RestResponse handle(RestRequest request, OperationContext context) {
        boolean sorted = request.paramAsBoolean("s", false);

        ObjectNode body = Json.object();
        ObjectNode rendered = body.putObject("patterns");
        patterns.ordered(sorted).forEach(rendered::put);
        return Json.response(RestStatus.OK, body, request);
    }
}
