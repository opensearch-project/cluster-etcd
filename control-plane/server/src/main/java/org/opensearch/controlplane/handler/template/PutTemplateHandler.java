/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.handler.template;

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

import java.io.IOException;

/**
 * Creates or updates a template, for any of the three kinds.
 *
 * <p>An upsert; a real cluster replaces an existing template and still answers
 * {@code {"acknowledged": true}}.
 *
 * <p>The required property and the error for omitting it both depend on the kind — see
 * {@link TemplateKind}. Legacy templates validate through a different path and report
 * {@code action_request_validation_exception} where the newer kinds report
 * {@code Required [index_patterns]}.
 *
 * <p>For composable templates, a put is rejected when its patterns overlap another template's at the
 * same priority — there would be no rule for which applies. Overlap at different priorities is allowed.
 *
 * <p>Settings are validated against the registry for existence and permission, but <b>not</b> for
 * dynamism: a template setting {@code index.number_of_shards} is the normal case, so static settings
 * are permitted here exactly as on {@code indices.create}.
 *
 * <p>The two rejections are shaped differently, which is worth knowing because it looks arbitrary.
 * Verified for all three kinds:
 *
 * <ul>
 *   <li>an unknown setting answers a <b>bare</b> {@code settings_exception} —
 *       {@code unknown setting [index.totally_bogus] ...}</li>
 *   <li>a private setting is <b>wrapped</b> —
 *       {@code invalid_index_template_exception}, {@code index_template [name] invalid, cause
 *       [Validation Failed: 1: private index setting [index.uuid] can not be set explicitly;]}. All
 *       three kinds say {@code index_template}, including component and legacy, the same quirk as the
 *       DELETE-missing message.</li>
 * </ul>
 *
 * <p>Still not validated: whether {@code composed_of} names component templates that exist. A missing
 * component is silently skipped during composition rather than rejected here.
 */
public final class PutTemplateHandler implements OperationHandler {

    private final TemplateKind kind;
    private final SettingsRegistry registry;

    PutTemplateHandler(TemplateKind kind, SettingsRegistry registry) {
        this.kind = kind;
        this.registry = registry;
    }

    @Override
    public String operationGroup() {
        return kind.putGroup();
    }

    @Override
    public RestResponse handle(RestRequest request, OperationContext context) {
        if (request.hasBody() == false) {
            return missingRequired(request);
        }

        JsonNode parsed;
        try {
            parsed = Json.parse(request.body());
        } catch (IOException e) {
            return ErrorResponses.parseFailure(request, "failed to parse template: " + e.getMessage());
        }
        if (parsed == null || parsed.isObject() == false) {
            return ErrorResponses.parseFailure(request, "template must be a JSON object");
        }
        if (parsed.hasNonNull(kind.requiredProperty()) == false) {
            return missingRequired(request);
        }

        String name = request.pathParam("name");
        ObjectNode definition = (ObjectNode) parsed;

        // Settings are checked before the overlap check, matching the order a real cluster reports them
        // in. Static settings are permitted -- a template setting number_of_shards is the normal case --
        // so only existence and permission are checked.
        ObjectNode block = kind.blockOf(definition);
        if (block != null) {
            Settings declared = Settings.forIndex(block.path("settings"));
            SettingsValidation.Problems problems =
                SettingsValidation.check(declared, registry, SettingsRegistry.Scope.INDEX, false);
            if (problems.unknown().isEmpty() == false) {
                String key = problems.unknown().getFirst();
                // Not wrapped in invalid_index_template_exception, unlike the private-setting case.
                // Verified for all three kinds: an unknown setting answers a bare settings_exception.
                return ErrorResponses.unknownSetting(request, key,
                    SettingsValidation.didYouMean(key, registry.exactKeys(SettingsRegistry.Scope.INDEX))
                        .orElse(null));
            }
            if (problems.notUserSettable().isEmpty() == false) {
                return ErrorResponses.privateSettingsRejected(request, problems.notUserSettable(), name);
            }
        }

        // Composable templates may not overlap another template's patterns at the same priority: with
        // equal priority there would be no rule for which one wins. Overlap at *different* priorities is
        // fine, since the higher one simply applies. Verified against a live cluster.
        if (kind.equals(TemplateKind.index())) {
            var conflicts = new TemplateComposition(context.store()).conflictingTemplates(name, definition);
            if (conflicts.isEmpty() == false) {
                return ErrorResponses.conflictingTemplatePriority(
                    request,
                    name,
                    TemplateComposition.patternsOf(definition),
                    conflicts,
                    definition.path("priority").asInt(0)
                );
            }
        }

        new TemplateStore(context.store(), kind).put(name, definition);
        return Json.acknowledged(request);
    }

    private RestResponse missingRequired(RestRequest request) {
        return kind.legacyValidation()
            ? ErrorResponses.legacyTemplateValidationFailed(request, "index patterns are missing")
            : ErrorResponses.requiredProperty(request, kind.requiredProperty());
    }
}
