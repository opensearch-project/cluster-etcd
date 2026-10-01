/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.handler.indices;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.opensearch.controlplane.handler.OperationContext;
import org.opensearch.controlplane.handler.OperationHandler;
import org.opensearch.controlplane.resolver.IndicesOptions;
import org.opensearch.controlplane.rest.Json;
import org.opensearch.controlplane.rest.NameExpression;
import org.opensearch.controlplane.rest.RestRequest;
import org.opensearch.controlplane.rest.RestResponse;
import org.opensearch.controlplane.rest.RestStatus;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * {@code GET /_mapping/field/{fields}} and {@code GET /{index}/_mapping/field/{fields}} — the mapping
 * of individual fields.
 *
 * <p>The response shape is peculiar and was verified rather than guessed. Each match is keyed by its
 * <b>full dotted name</b>, carries that name again as {@code full_name}, and holds a {@code mapping}
 * object keyed by the field's <b>leaf name</b>:
 *
 * <pre>
 * GET /i/_mapping/field/user.name
 * {"i": {"mappings": {"user.name": {"full_name": "user.name",
 *                                   "mapping": {"name": {"type": "text"}}}}}}
 * </pre>
 *
 * <p>Note the two names differ — {@code user.name} outside, {@code name} inside. Rules confirmed
 * against 3.9.0:
 *
 * <ul>
 *   <li><b>Only leaf fields match.</b> Asking for an object field answers nothing:
 *       {@code /_mapping/field/user} is an empty result even though {@code user.name} is a match.</li>
 *   <li><b>Multi-fields are leaves.</b> {@code title.raw} declared under {@code fields} resolves like
 *       any other, keyed {@code title.raw} with its {@code mapping} keyed {@code raw}.</li>
 *   <li><b>A field that matches nothing is a 200</b>, with the index present and
 *       {@code "mappings": {}} — the index is not dropped.</li>
 *   <li>Names are comma-separated and take {@code *} wildcards, like every other name list.</li>
 * </ul>
 *
 * <h2>Metadata fields are not reported</h2>
 *
 * <p>The one real gap, and it shows under a wildcard. On a real cluster {@code /_mapping/field/*} also
 * returns the metadata fields every index has — {@code _index}, {@code _routing}, {@code _doc_count},
 * {@code _feature} and the rest — each with an empty {@code mapping}. Those come from the mapping
 * engine's field registry, not from anything the user declared, and this service has no such registry:
 * the list is version- and plugin-dependent, so a hardcoded copy would drift exactly the way a
 * hardcoded settings registry would. Only declared fields are reported. See the README.
 */
public final class GetFieldMappingHandler implements OperationHandler {

    @Override
    public String operationGroup() {
        return "indices.get_field_mapping";
    }

    @Override
    public RestResponse handle(RestRequest request, OperationContext context) {
        IndexExpressions.Resolved resolved =
            IndexExpressions.resolve(request, context.store(), IndicesOptions.strictExpandOpen());
        if (resolved.failed()) {
            return resolved.failure();
        }

        List<Pattern> wanted = NameExpression.parse(request.pathParam("fields"));
        IndexStore indices = new IndexStore(context.store());

        ObjectNode body = Json.object();
        resolved.indices().stream().sorted().forEach(index -> indices.get(index).ifPresent(metadata -> {
            Map<String, JsonNode> leaves = new LinkedHashMap<>();
            collectLeaves("", metadata.path("mappings"), leaves);

            ObjectNode matches = Json.object();
            leaves.forEach((fullName, mapping) -> {
                if (NameExpression.matchesAny(wanted, fullName) == false) {
                    return;
                }
                ObjectNode described = Json.object();
                described.put("full_name", fullName);
                ObjectNode byLeafName = Json.object();
                byLeafName.set(leafOf(fullName), mapping.deepCopy());
                described.set("mapping", byLeafName);
                matches.set(fullName, described);
            });

            ObjectNode entry = Json.object();
            entry.set("mappings", matches);
            body.set(index, entry);
        }));
        return Json.response(RestStatus.OK, body, request);
    }

    /**
     * Walks {@code properties} and {@code fields} to the leaves, accumulating dotted full names.
     *
     * <p>A node carrying {@code properties} is an object field: it contributes its children and not
     * itself, which is why asking for {@code user} matches nothing. A node carrying {@code fields}
     * contributes both itself and its multi-fields.
     */
    private static void collectLeaves(String prefix, JsonNode node, Map<String, JsonNode> out) {
        if (node.isObject() == false) {
            return;
        }
        JsonNode properties = node.get("properties");
        if (properties != null && properties.isObject()) {
            properties.properties().forEach(field ->
                collectLeaves(prefix + field.getKey() + ".", field.getValue(), out));
        }
        JsonNode multiFields = node.get("fields");
        if (multiFields != null && multiFields.isObject()) {
            multiFields.properties().forEach(field ->
                collectLeaves(prefix + field.getKey() + ".", field.getValue(), out));
        }
        // The root of the mappings document is not a field; only nested nodes are.
        if (prefix.isEmpty() == false && properties == null) {
            out.put(prefix.substring(0, prefix.length() - 1), node);
        }
    }

    private static String leafOf(String fullName) {
        int lastDot = fullName.lastIndexOf('.');
        return lastDot < 0 ? fullName : fullName.substring(lastDot + 1);
    }
}
