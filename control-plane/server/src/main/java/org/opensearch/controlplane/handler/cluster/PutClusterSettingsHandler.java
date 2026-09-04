/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.handler.cluster;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.opensearch.controlplane.capability.SettingsRegistry;
import org.opensearch.controlplane.handler.OperationContext;
import org.opensearch.controlplane.handler.OperationHandler;
import org.opensearch.controlplane.metadata.Settings;
import org.opensearch.controlplane.metadata.SettingsValidation;
import org.opensearch.controlplane.rest.ErrorResponses;
import org.opensearch.controlplane.rest.Json;
import org.opensearch.controlplane.rest.RestRequest;
import org.opensearch.controlplane.rest.RestResponse;
import org.opensearch.controlplane.rest.RestStatus;

import java.io.IOException;
import java.util.Set;

/**
 * {@code PUT /_cluster/settings} — sets cluster settings in either scope.
 *
 * <p>The two scopes are stored separately and otherwise uninterpreted; see {@link ClusterSettingsStore}
 * for why that is the right division of labour with the data plane.
 *
 * <h2>The response echoes the request, not the state</h2>
 *
 * <p>Easy to get wrong and worth stating. The body is
 * {@code {"acknowledged": true, "persistent": {...}, "transient": {...}}}, and those blocks hold
 * <b>only what this request assigned</b> — not the cluster's full settings. Verified: with
 * {@code cluster.max_shards_per_node} already set persistently, a request setting
 * {@code cluster.blocks.read_only} answers only the latter, and a request that <em>removes</em> a
 * setting answers {@code {}} for that scope because it assigned nothing.
 *
 * <h2>Rejections</h2>
 *
 * <p>Different wording from the index path, and the scope word is part of the message — verified for
 * both scopes:
 *
 * <ul>
 *   <li>unknown — {@code settings_exception},
 *       {@code persistent setting [cluster.totally_bogus], not recognized}</li>
 *   <li>static — {@code settings_exception},
 *       {@code persistent setting [cluster.name], not dynamically updateable}</li>
 *   <li>nothing to set — {@code action_request_validation_exception},
 *       {@code Validation Failed: 1: no settings to update;}. Reached by an empty body object and,
 *       notably, by a body whose only keys are neither scope: {@code {"bogus":{...}}} is that error
 *       rather than an unknown-scope one.</li>
 * </ul>
 *
 * <p>A null removes a setting rather than storing one, as on the index path, and is therefore not
 * validated — removing a setting is not setting it.
 */
public final class PutClusterSettingsHandler implements OperationHandler {

    private final SettingsRegistry registry;

    public PutClusterSettingsHandler(SettingsRegistry registry) {
        this.registry = registry;
    }

    @Override
    public String operationGroup() {
        return "cluster.put_settings";
    }

    @Override
    public RestResponse handle(RestRequest request, OperationContext context) {
        if (request.hasBody() == false) {
            return ErrorResponses.noSettingsToUpdate(request);
        }
        JsonNode body;
        try {
            body = Json.parse(request.body());
        } catch (IOException e) {
            return ErrorResponses.parseFailure(request, "failed to parse settings: " + e.getMessage());
        }
        if (body == null || body.isObject() == false) {
            return ErrorResponses.parseFailure(request, "settings must be a JSON object");
        }

        Scope[] scopes = {
            new Scope(ClusterSettingsStore.PERSISTENT, Settings.of(body.path(ClusterSettingsStore.PERSISTENT))),
            new Scope(ClusterSettingsStore.TRANSIENT, Settings.of(body.path(ClusterSettingsStore.TRANSIENT)))
        };
        if (scopes[0].requested().isEmpty() && scopes[1].requested().isEmpty()) {
            // Covers an empty body and one naming neither scope; a real cluster does not distinguish.
            return ErrorResponses.noSettingsToUpdate(request);
        }

        for (Scope scope : scopes) {
            RestResponse rejection = validate(request, scope);
            if (rejection != null) {
                return rejection;
            }
        }

        ClusterSettingsStore settings = new ClusterSettingsStore(context.store());
        ClusterSettingsStore.Current current = settings.read();

        Settings persistent = apply(current.persistent(), scopes[0]);
        Settings transientSettings = apply(current.transientSettings(), scopes[1]);
        if (settings.write(persistent, transientSettings, current.revision()) == false) {
            return ErrorResponses.concurrentUpdate(request, "_cluster/settings");
        }

        boolean flatSettings = request.paramAsBoolean("flat_settings", false);
        ObjectNode responseBody = Json.object();
        responseBody.put("acknowledged", true);
        // Only what this request assigned, which is what a real cluster echoes.
        responseBody.set(ClusterSettingsStore.PERSISTENT, scopes[0].assignments().toJson(flatSettings));
        responseBody.set(ClusterSettingsStore.TRANSIENT, scopes[1].assignments().toJson(flatSettings));
        return Json.response(RestStatus.OK, responseBody, request);
    }

    private RestResponse validate(RestRequest request, Scope scope) {
        SettingsValidation.Problems problems = SettingsValidation.check(
            scope.assignments(), registry, SettingsRegistry.Scope.CLUSTER, true);

        if (problems.unknown().isEmpty() == false) {
            return ErrorResponses.clusterSettingRejected(
                request, scope.name(), problems.unknown().getFirst(), "not recognized");
        }
        if (problems.notDynamic().isEmpty() == false) {
            return ErrorResponses.clusterSettingRejected(
                request, scope.name(), problems.notDynamic().getFirst(), "not dynamically updateable");
        }
        return null;
    }

    private static Settings apply(Settings current, Scope scope) {
        return current.mergedWith(scope.assignments()).without(scope.removals());
    }

    /** One scope of the request: what it assigns, and what it removes. */
    private record Scope(String name, Settings requested) {

        Settings assignments() {
            return requested.withoutNulls();
        }

        Set<String> removals() {
            return requested.nullKeys();
        }
    }
}
