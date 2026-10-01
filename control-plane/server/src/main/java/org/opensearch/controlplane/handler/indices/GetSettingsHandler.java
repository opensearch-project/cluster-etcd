/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.handler.indices;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.opensearch.controlplane.handler.OperationContext;
import org.opensearch.controlplane.handler.OperationHandler;
import org.opensearch.controlplane.metadata.Settings;
import org.opensearch.controlplane.resolver.IndicesOptions;
import org.opensearch.controlplane.rest.Json;
import org.opensearch.controlplane.rest.NameExpression;
import org.opensearch.controlplane.rest.RestRequest;
import org.opensearch.controlplane.rest.RestResponse;
import org.opensearch.controlplane.rest.RestStatus;

import java.util.List;
import java.util.regex.Pattern;

/**
 * {@code GET /_settings}, {@code /_settings/{name}}, {@code /{index}/_settings} and
 * {@code /{index}/_settings/{name}} — an index's settings.
 *
 * <p>Mostly rendering, now that {@link Settings} holds the canonical form: resolve the index
 * expression, read each index's settings, optionally filter by setting name, and answer
 * {@code {"<index>": {"settings": {...}}}}. Everything below was verified against OpenSearch 3.9.0.
 *
 * <h2>The setting-name filter</h2>
 *
 * <p>{@code {name}} is a comma-separated list of {@code *} globs matched against the <b>full dotted
 * key</b>. The asymmetry with writing settings is the part worth knowing: a write may omit the
 * {@code index.} prefix and have it supplied, but <b>a filter may not</b>. So
 * {@code /_settings/index.number_of_shards} and {@code /_settings/index.number_*} both work while
 * {@code /_settings/number_of_shards} answers a bare {@code {}}.
 *
 * <p><b>An index whose filtered settings come out empty is omitted from the response entirely</b>,
 * rather than appearing as {@code "settings": {}}. With nothing left, the whole body is {@code {}} —
 * and still a 200, because the indices did resolve. Only the filter can produce that: an unfiltered
 * index always has settings.
 *
 * <h2>Not implemented</h2>
 *
 * <p><b>{@code include_defaults}.</b> A real cluster answers a sibling {@code defaults} block holding
 * every registered setting's default — around 2.6 kB for a single index. Not implemented, and the
 * reason is a boundary rather than a limitation: <b>the control plane owns the settings a user
 * explicitly set, and computed values belong to the layer that computes them.</b> A data plane node can
 * already answer {@code /{index}/_settings?local=true} with the fully resolved settings, defaults
 * included, so a settings registry here would duplicate one that exists — and a duplicated registry
 * drifts. The same reasoning governs processor and analyzer validation; see the README on data plane
 * capabilities.
 *
 * <p>The parameter is accepted and ignored rather than rejected, so a client that sends it still gets a
 * correct response, minus the block. Recorded as a considered divergence.
 *
 * <p>One consequence: with {@code include_defaults=true} a real cluster emits an index entry even when
 * both blocks are empty, where the omit-when-empty rule above would drop it. Since no {@code defaults}
 * block is produced here, the rule applies unconditionally.
 */
public final class GetSettingsHandler implements OperationHandler {

    @Override
    public String operationGroup() {
        return "indices.get_settings";
    }

    @Override
    public RestResponse handle(RestRequest request, OperationContext context) {
        // Wildcards expand to open indices, a missing concrete name is a 404, and a wildcard matching
        // nothing is a 200. Verified.
        IndexExpressions.Resolved resolved =
            IndexExpressions.resolve(request, context.store(), IndicesOptions.strictExpandOpen());
        if (resolved.failed()) {
            return resolved.failure();
        }

        boolean flatSettings = request.paramAsBoolean("flat_settings", false);
        String nameExpression = request.pathParam("name");
        List<Pattern> nameFilter = nameExpression == null ? null : NameExpression.parse(nameExpression);

        IndexStore store = new IndexStore(context.store());
        ObjectNode body = Json.object();
        // Sorted, so the response is stable across calls; the store scans in key order anyway.
        resolved.indices().stream().sorted().forEach(index -> store.get(index).ifPresent(metadata -> {
            Settings settings = IndexStore.reportedSettings(metadata);
            if (nameFilter != null) {
                settings = settings.retainingKeysMatching(nameFilter);
            }
            if (settings.isEmpty()) {
                return;
            }
            ObjectNode entry = Json.object();
            entry.set("settings", settings.toJson(flatSettings));
            body.set(index, entry);
        }));
        return Json.response(RestStatus.OK, body, request);
    }
}
