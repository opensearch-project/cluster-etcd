/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.handler.indices;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.opensearch.controlplane.handler.OperationContext;
import org.opensearch.controlplane.handler.OperationHandler;
import org.opensearch.controlplane.handler.template.TemplateComposition;
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
 * <p><b>Legacy template application.</b> A matching composable template is applied — see
 * {@code TemplateComposition} for the precedence rules — but legacy {@code _template} definitions are
 * not. A real cluster falls back to them when no composable template matches. An index created here
 * while a matching legacy template exists will differ from one created on a real cluster.
 *
 * <p><b>Settings validation.</b> Unknown or malformed settings are stored rather than rejected. A
 * real cluster validates against registered setting definitions and fails with 400.
 *
 * <p><b>Server-supplied settings.</b> A real cluster writes {@code index.uuid},
 * {@code index.creation_date}, {@code index.provided_name} and {@code index.version.created} into the
 * index's settings as part of creating it. The first three are held as top-level metadata fields here
 * and rendered back into {@code indices.get_settings}; {@code index.version.created} is the Lucene and
 * OpenSearch version the index was built with, which this service has no honest value for.
 */
public final class CreateIndexHandler implements OperationHandler {

    /**
     * OpenSearch's defaults, needed by the shard manager to allocate anything at all.
     *
     * <p>Strings because that is what a settings value is — see {@link org.opensearch.controlplane.metadata.Settings}.
     */
    private static final String DEFAULT_NUMBER_OF_SHARDS = "1";
    private static final String DEFAULT_NUMBER_OF_REPLICAS = "1";

    /**
     * Replication is per index and chosen by whoever creates it: {@code DOCUMENT} replicates each
     * operation to every copy, {@code SEGMENT} ships segments from the primary. The request decides,
     * and {@code DOCUMENT} is the default a real cluster applies when it does not say.
     *
     * <p>Changing it afterwards is not supported here — it is a static setting, so it belongs with
     * {@code indices.put_settings} and the static/dynamic registry that operation needs anyway.
     */
    private static final String DEFAULT_REPLICATION_TYPE = "DOCUMENT";

    private static final String SETTING_SHARDS = "index.number_of_shards";
    private static final String SETTING_REPLICAS = "index.number_of_replicas";
    private static final String SETTING_REPLICATION_TYPE = "index.replication.type";

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

        // Composition first: a matching composable template contributes settings, mappings and aliases,
        // and the request body overrides whatever it supplied.
        TemplateComposition.Composed effective = TemplateComposition.composeFor(context.store(), index, requested);

        IndexStore indices = new IndexStore(context.store());
        String uuid = IndexStore.newUuid();
        ObjectNode metadata = buildMetadata(index, uuid, effective, context.nowEpochMillis());

        if (indices.createIfAbsent(metadata, index).isEmpty()) {
            // Lost the create-if-absent, so something else got there first. Report its UUID, not the
            // one we generated and discarded -- the client should see what actually exists.
            String existingUuid = indices.get(index)
                .map(existing -> existing.path("uuid").asText("_na_"))
                .orElse("_na_");
            return ErrorResponses.indexAlreadyExists(request, index, existingUuid);
        }

        // Aliases are their own resource, so they are written separately rather than embedded. Not
        // atomic with the index write: a failure here leaves an index without its aliases, which a
        // reconciliation pass can detect. The alternative -- embedding them -- would cost the atomic
        // alias swap that the separate layout buys.
        AliasStore aliasStore = new AliasStore(context.store());
        effective.aliases().properties().forEach(alias -> {
            ObjectNode definition = aliasStore.get(alias.getKey()).orElseGet(AliasStore::emptyDefinition);
            ((ObjectNode) definition.get(AliasStore.INDICES)).set(index, alias.getValue().deepCopy());
            aliasStore.put(alias.getKey(), definition);
        });

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

    private static ObjectNode buildMetadata(
        String index,
        String uuid,
        TemplateComposition.Composed effective,
        long nowEpochMillis
    ) {
        ObjectNode metadata = Json.object();
        metadata.put("index", index);
        metadata.put("uuid", uuid);
        // OpenSearch stores creation_date as epoch millis in a string.
        metadata.put("creation_date", Long.toString(nowEpochMillis));
        metadata.put("state", "open");
        // Stored flat, which is the canonical form -- see Settings. The shard manager reads these keys
        // directly, and indices.get_settings renders them nested on the way out.
        metadata.set("settings", effective.settings()
            .withDefault(SETTING_SHARDS, DEFAULT_NUMBER_OF_SHARDS)
            .withDefault(SETTING_REPLICAS, DEFAULT_NUMBER_OF_REPLICAS)
            .withDefault(SETTING_REPLICATION_TYPE, DEFAULT_REPLICATION_TYPE)
            .toJson(true));
        metadata.set("mappings", effective.mappings());
        return metadata;
    }
}
