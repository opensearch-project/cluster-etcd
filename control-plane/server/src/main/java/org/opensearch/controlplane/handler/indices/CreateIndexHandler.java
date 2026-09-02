/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.handler.indices;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.opensearch.controlplane.handler.OperationContext;
import org.opensearch.controlplane.handler.OperationHandler;
import org.opensearch.controlplane.rest.ErrorResponses;
import org.opensearch.controlplane.rest.Json;
import org.opensearch.controlplane.rest.RestRequest;
import org.opensearch.controlplane.rest.RestResponse;
import org.opensearch.controlplane.rest.RestStatus;

import java.io.IOException;
import java.util.Optional;

/**
 * {@code PUT /{index}} — creates an index.
 *
 * <p>The first operation that hands work off to another layer. It validates the request, writes index
 * metadata, records an index management task, and returns — it does not wait for shards, because
 * allocating them is the shard manager's job and creating them is the data plane's.
 *
 * <h2>shards_acknowledged</h2>
 *
 * <p>The one field where this deliberately differs from a real cluster, and it is worth understanding
 * rather than treating as a bug. In OpenSearch, {@code shards_acknowledged} reports whether
 * {@code wait_for_active_shards} copies became active before the timeout. A real cluster creates
 * shards synchronously enough to answer {@code true}; this service returns before any shard exists,
 * so it cannot honestly claim that.
 *
 * <p>Rather than lying, the field is derived from what was actually asked:
 *
 * <ul>
 *   <li>{@code wait_for_active_shards=0} — nothing was required, so the requirement is trivially
 *       met: {@code true}.</li>
 *   <li>anything else (including the default of 1) — we did not wait and cannot confirm:
 *       {@code false}.</li>
 * </ul>
 *
 * <p>{@code false} is not an error signal in OpenSearch either: it is what a real cluster returns
 * when the wait times out, and means "the index exists, its shards are not confirmed active yet".
 * That is precisely true here. Clients that treat it as failure will need the eventual task-status
 * API to find out when shards did become active.
 *
 * <h2>Not yet implemented</h2>
 *
 * <p><b>Template resolution.</b> A real create applies matching composable index templates and
 * legacy templates, merging their settings, mappings and aliases with the request in a defined
 * precedence order. None of that happens here — the request body is stored as given, with only the
 * shard-count defaults filled in. An index created through this handler will therefore differ from
 * one created on a cluster with matching templates. This is the largest single gap in the operation
 * and wants {@code MetadataIndexTemplateService}'s composition rules ported before it is trustworthy.
 *
 * <p><b>Settings validation.</b> Unknown or malformed settings are stored rather than rejected. A
 * real cluster validates against registered setting definitions and fails with 400.
 */
public final class CreateIndexHandler implements OperationHandler {

    /** OpenSearch's defaults, needed by the shard manager to allocate anything at all. */
    private static final int DEFAULT_NUMBER_OF_SHARDS = 1;
    private static final int DEFAULT_NUMBER_OF_REPLICAS = 1;

    private static final String SETTING_SHARDS = "index.number_of_shards";
    private static final String SETTING_REPLICAS = "index.number_of_replicas";

    @Override
    public String operationGroup() {
        return "indices.create";
    }

    @Override
    public RestResponse handle(RestRequest request, OperationContext context) {
        String index = request.pathParam("index");

        Optional<String> invalid = IndexNameValidator.validate(index);
        if (invalid.isPresent()) {
            return ErrorResponses.invalidIndexName(request, index, invalid.get());
        }

        // A body is optional: PUT /my-index with nothing at all is a valid create.
        ObjectNode requested;
        if (request.hasBody()) {
            JsonNode parsed;
            try {
                parsed = Json.parse(request.body());
            } catch (IOException e) {
                return ErrorResponses.parseFailure(request, "failed to parse index configuration: " + e.getMessage());
            }
            if (parsed == null || parsed.isObject() == false) {
                return ErrorResponses.parseFailure(request, "index configuration must be a JSON object");
            }
            requested = (ObjectNode) parsed;
        } else {
            requested = Json.object();
        }

        IndexStore indices = new IndexStore(context.store());
        String uuid = IndexStore.newUuid();
        ObjectNode metadata = buildMetadata(index, uuid, requested, context.nowEpochMillis());

        if (indices.createIfAbsent(metadata, index).isEmpty()) {
            // Lost the create-if-absent, so something else got there first. Report its UUID, not the
            // one we generated and discarded -- the client should see what actually exists.
            String existingUuid = indices.get(index)
                .map(existing -> existing.path("uuid").asText("_na_"))
                .orElse("_na_");
            return ErrorResponses.indexAlreadyExists(request, index, existingUuid);
        }

        new IndexManagementTasks(context.store()).submit(
            IndexManagementTasks.TYPE_CREATE_INDEX,
            index,
            uuid,
            context.nowEpochMillis()
        );

        ObjectNode body = Json.object();
        body.put("acknowledged", true);
        body.put("shards_acknowledged", shardsAcknowledged(request));
        body.put("index", index);
        return Json.response(RestStatus.OK, body, request);
    }

    /** See the class javadoc: derived from what was asked rather than asserted. */
    private static boolean shardsAcknowledged(RestRequest request) {
        return "0".equals(request.param("wait_for_active_shards"));
    }

    private static ObjectNode buildMetadata(String index, String uuid, ObjectNode requested, long nowEpochMillis) {
        ObjectNode metadata = Json.object();
        metadata.put("index", index);
        metadata.put("uuid", uuid);
        // OpenSearch stores creation_date as epoch millis in a string.
        metadata.put("creation_date", Long.toString(nowEpochMillis));
        metadata.put("state", "open");
        metadata.set("settings", normalizeSettings(requested.path("settings")));
        metadata.set("mappings", requested.has("mappings") ? requested.get("mappings").deepCopy() : Json.object());
        metadata.set("aliases", requested.has("aliases") ? requested.get("aliases").deepCopy() : Json.object());
        return metadata;
    }

    /**
     * Flattens settings to the {@code index.}-prefixed form and fills in the shard defaults.
     *
     * <p>OpenSearch accepts both {@code {"number_of_shards": 2}} and
     * {@code {"index": {"number_of_shards": 2}}}, and normalises internally. The shard manager needs
     * one predictable shape to read, so both are accepted and one is stored.
     *
     * <p>Only the two shard settings are understood. Everything else is carried through unchanged and
     * unvalidated; see the class javadoc.
     */
    private static ObjectNode normalizeSettings(JsonNode requestedSettings) {
        ObjectNode settings = Json.object();

        if (requestedSettings.isObject()) {
            requestedSettings.properties().forEach(entry -> {
                if ("index".equals(entry.getKey()) && entry.getValue().isObject()) {
                    entry.getValue().properties()
                        .forEach(nested -> settings.set("index." + nested.getKey(), nested.getValue().deepCopy()));
                } else if (entry.getKey().startsWith("index.")) {
                    settings.set(entry.getKey(), entry.getValue().deepCopy());
                } else {
                    settings.set("index." + entry.getKey(), entry.getValue().deepCopy());
                }
            });
        }

        if (settings.has(SETTING_SHARDS) == false) {
            settings.put(SETTING_SHARDS, DEFAULT_NUMBER_OF_SHARDS);
        }
        if (settings.has(SETTING_REPLICAS) == false) {
            settings.put(SETTING_REPLICAS, DEFAULT_NUMBER_OF_REPLICAS);
        }
        return settings;
    }
}
