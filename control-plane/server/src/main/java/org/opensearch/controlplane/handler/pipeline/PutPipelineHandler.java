/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.handler.pipeline;

import com.fasterxml.jackson.databind.JsonNode;
import org.opensearch.controlplane.capability.DataPlaneCapabilities;
import org.opensearch.controlplane.capability.ProcessorKind;
import org.opensearch.controlplane.handler.OperationContext;
import org.opensearch.controlplane.handler.OperationHandler;
import org.opensearch.controlplane.rest.ErrorResponses;
import org.opensearch.controlplane.rest.Json;
import org.opensearch.controlplane.rest.RestRequest;
import org.opensearch.controlplane.rest.RestResponse;
import org.opensearch.controlplane.store.WriteLimits;

import java.io.IOException;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

/**
 * {@code PUT /_ingest/pipeline/{id}} and {@code PUT /_search/pipeline/{id}} — creates or updates a
 * pipeline definition.
 *
 * <p>An unconditional upsert, matching OpenSearch: putting over an existing pipeline replaces it and
 * still returns {@code 200 {"acknowledged": true}}.
 *
 * <p>The two kinds differ in exactly one way. Ingest requires {@code processors} even though the
 * spec's {@code Pipeline} schema does not mark it required — a live cluster rejects {@code {}} with
 * {@code [processors] required property is missing}, which is a spec bug worth reporting upstream. A
 * search pipeline has three independently optional processor lists, so {@code {}} is accepted. Both
 * verified against OpenSearch 3.9.0.
 *
 * <h2>Processor type validation</h2>
 *
 * <p>Named processor types are checked against {@link DataPlaneCapabilities}, so a definition that the
 * data plane could never run is refused here rather than at execution time. This matters because the
 * spec describes the whole distribution: {@code normalization-processor} is spec-valid but only exists
 * where the neural-search plugin is installed.
 *
 * <p>Validation is skipped entirely when nothing has been declared — see
 * {@link org.opensearch.controlplane.capability.PermissiveCapabilities} for why that is the default.
 *
 * <p>Processor arrays are found recursively, so an {@code on_failure} list nested inside a processor's
 * own configuration is checked too, which matches what a live cluster does. Not yet checked: a
 * processor referenced under a singular key, as {@code foreach} does with its inner
 * {@code processor} — those are accepted regardless.
 */
public final class PutPipelineHandler implements OperationHandler {

    private final PipelineKind kind;
    private final DataPlaneCapabilities capabilities;

    public PutPipelineHandler(PipelineKind kind, DataPlaneCapabilities capabilities) {
        this.kind = kind;
        this.capabilities = capabilities;
    }

    @Override
    public String operationGroup() {
        return kind.putGroup();
    }

    @Override
    public RestResponse handle(RestRequest request, OperationContext context) {
        if (request.hasBody() == false) {
            return ErrorResponses.parseFailure(request, "request body or source parameter is required");
        }

        JsonNode definition;
        try {
            definition = Json.parse(request.body());
        } catch (IOException e) {
            return ErrorResponses.parseFailure(request, "failed to parse pipeline definition: " + e.getMessage());
        }
        if (definition == null || definition.isObject() == false) {
            return ErrorResponses.parseFailure(request, "pipeline definition must be a JSON object");
        }
        // Upstream caps pipeline ids at 512 UTF-8 bytes, ahead of parsing the definition; the id is a key
        // segment here, so the cap is the store's 255 and the message is upstream's with that number in it.
        String id = request.pathParam("id");
        int idBytes = WriteLimits.utf8Bytes(id);
        if (idBytes > WriteLimits.MAX_NAME_BYTES) {
            return ErrorResponses.illegalArgument(request, String.format(
                Locale.ROOT,
                "%s id [%s] exceeds maximum length of %d UTF-8 bytes (actual: %d bytes)",
                kind.errorStyle() == PipelineKind.ProcessorErrorStyle.INGEST ? "Pipeline" : "Search Pipeline",
                id,
                WriteLimits.MAX_NAME_BYTES,
                idBytes
            ));
        }
        if (kind.hasRequiredProperty() && definition.hasNonNull(kind.requiredProperty()) == false) {
            return ErrorResponses.missingRequiredProperty(request, kind.requiredProperty());
        }

        Optional<RestResponse> unsupported = validateProcessors(request, definition);
        if (unsupported.isPresent()) {
            return unsupported.get();
        }

        // Stored canonically rather than verbatim so that output formatting is a function of ?pretty
        // alone, not of however the client happened to indent the request.
        new PipelineDefinitions(context.store(), kind).put(id, Json.toBytes(definition));
        return Json.acknowledged(request);
    }

    /**
     * Walks the definition for processor arrays and rejects the first type the data plane does not
     * support, matching the server's behaviour of failing on one processor rather than listing all.
     */
    private Optional<RestResponse> validateProcessors(RestRequest request, JsonNode definition) {
        if (capabilities.isEnforcing() == false) {
            return Optional.empty();
        }
        return findUnsupported(definition).map(type -> switch (kind.errorStyle()) {
            case INGEST -> ErrorResponses.unknownIngestProcessor(request, type);
            case SEARCH -> ErrorResponses.unknownSearchProcessor(request, type);
        });
    }

    /** @return the first unsupported processor type found anywhere in the tree */
    private Optional<String> findUnsupported(JsonNode node) {
        if (node.isObject() == false) {
            return Optional.empty();
        }
        for (Map.Entry<String, JsonNode> field : node.properties()) {
            ProcessorKind processorKind = kind.processorFields().get(field.getKey());
            if (processorKind != null && field.getValue().isArray()) {
                for (JsonNode entry : field.getValue()) {
                    // By OpenSearch convention a processor entry is an object whose single key names
                    // the type, with its configuration as the value.
                    for (Map.Entry<String, JsonNode> processor : entry.properties()) {
                        if (capabilities.supportsProcessor(processorKind, processor.getKey()) == false) {
                            return Optional.of(processor.getKey());
                        }
                        // Recurse into the configuration to catch a nested on_failure list.
                        Optional<String> nested = findUnsupported(processor.getValue());
                        if (nested.isPresent()) {
                            return nested;
                        }
                    }
                }
            }
        }
        return Optional.empty();
    }
}
