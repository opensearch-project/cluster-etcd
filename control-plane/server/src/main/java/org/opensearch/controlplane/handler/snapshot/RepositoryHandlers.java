/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.handler.snapshot;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.opensearch.controlplane.handler.OperationContext;
import org.opensearch.controlplane.handler.OperationHandler;
import org.opensearch.controlplane.metadata.Settings;
import org.opensearch.controlplane.rest.ErrorResponses;
import org.opensearch.controlplane.rest.Json;
import org.opensearch.controlplane.rest.RestRequest;
import org.opensearch.controlplane.rest.RestResponse;
import org.opensearch.controlplane.rest.RestStatus;
import org.opensearch.controlplane.store.WriteLimits;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

/**
 * The three repository-configuration operations: {@code PUT}, {@code GET} and {@code DELETE} on
 * {@code /_snapshot/{repository}}.
 *
 * <p>One class for all three because they share a store and a not-found shape, and because the
 * interesting behaviour is in how they differ over one thing — what a name expression means, which is
 * genuinely different between the read and the delete. See {@link RepositoryStore}.
 *
 * <p>These are the only snapshot operations that need nothing but the key/value store: they handle a
 * repository's <em>definition</em>, not its contents. Everything else in the namespace has to read the
 * repository itself, which is a second outbound seam this service does not yet have.
 *
 * <h2>Verified</h2>
 *
 * <ul>
 *   <li><b>Missing repository</b> — {@code 404 repository_missing_exception}, {@code [name] missing}, on
 *       both {@code GET} and {@code DELETE}. What counts as missing differs between the two, and neither
 *       follows the convention the rest of this service uses; see {@link RepositoryStore}.</li>
 *   <li><b>No {@code type}</b> — {@code 400 action_request_validation_exception},
 *       {@code Validation Failed: 1: type is missing;}. An empty body object gives the same; a request
 *       with no body at all gives {@code 400 parse_exception}, {@code request body is required}.</li>
 *   <li><b>Unknown type</b> — <b>{@code 500}</b> {@code repository_exception},
 *       {@code [name] repository type [nonesuch] does not exist}. A 500 for what is plainly a client
 *       error, and the second place in this API where that happens — {@code source} without
 *       {@code source_content_type} is the other.</li>
 * </ul>
 *
 * <h2>Repository types are not validated</h2>
 *
 * <p>So that 500 is not reproduced: which types exist depends on installed plugins
 * ({@code repository-s3}, {@code repository-azure}, {@code repository-gcs}), exactly as processor types
 * do. The difference is that a cluster reports its processors under {@code GET /_nodes} and reports
 * <b>nothing</b> about repository types anywhere — verified — so there is no list to generate one from.
 *
 * <p>Rather than hardcode one that would drift, any type is accepted and the data plane rejects an
 * unusable repository when something first tries to use it. This is the same argument as
 * {@code --capabilities}, at the stage processors were at before the endpoint existed; the settings
 * registry shows the shape of the fix, which is a data plane endpoint listing registered types.
 */
public final class RepositoryHandlers {

    private RepositoryHandlers() {}

    public static List<OperationHandler> handlers() {
        return List.of(new Create(), new Get(), new Delete());
    }

    /** {@code PUT /_snapshot/{repository}} — an upsert, as with templates and pipelines. */
    static final class Create implements OperationHandler {

        @Override
        public String operationGroup() {
            return "snapshot.create_repository";
        }

        @Override
        public RestResponse handle(RestRequest request, OperationContext context) {
            if (request.hasBody() == false) {
                return ErrorResponses.parseFailure(request, "request body is required");
            }
            JsonNode body;
            try {
                body = Json.parse(request.body());
            } catch (IOException e) {
                return ErrorResponses.parseFailure(request, "failed to parse repository: " + e.getMessage());
            }
            if (body == null || body.isObject() == false) {
                return ErrorResponses.parseFailure(request, "repository must be a JSON object");
            }

            String type = body.path(RepositoryStore.TYPE).asText(null);
            if (type == null || type.isBlank()) {
                return ErrorResponses.validationFailed(request, "type is missing");
            }

            // After the request's own validation, where RepositoriesService.validate checks upstream's name
            // rules. Upstream has no length rule; the name is a key segment here.
            String name = request.pathParam("repository");
            Optional<String> tooLong = WriteLimits.nameTooLong(name);
            if (tooLong.isPresent()) {
                return ErrorResponses.repositoryRejected(request, name, "is too long, " + tooLong.get());
            }

            // No implicit prefix: a repository setting is written out in full, unlike an index setting.
            new RepositoryStore(context.store()).put(
                name,
                type,
                Settings.of(body.path(RepositoryStore.SETTINGS))
            );
            return Json.acknowledged(request);
        }
    }

    /**
     * {@code GET /_snapshot} and {@code GET /_snapshot/{repository}}.
     *
     * <p>Answers a name-keyed map of definitions. Absent or {@code _all} means everything and is a 200
     * even when there is nothing. See {@link RepositoryStore#resolveForRead} for the rest, which depends
     * on whether the cluster has ever had a repository.
     */
    static final class Get implements OperationHandler {

        @Override
        public String operationGroup() {
            return "snapshot.get_repository";
        }

        @Override
        public RestResponse handle(RestRequest request, OperationContext context) {
            String expression = request.pathParam("repository");
            RepositoryStore repositories = new RepositoryStore(context.store());

            RepositoryStore.Resolution resolved = repositories.resolveForRead(expression);
            if (resolved.failed()) {
                return ErrorResponses.repositoryMissing(request, resolved.missing());
            }

            ObjectNode body = Json.object();
            resolved.found().forEach((name, definition) -> body.set(name, rendered(definition)));
            return Json.response(RestStatus.OK, body, request);
        }

        /** Stored flat, reported nested, as with every other settings document. */
        private static ObjectNode rendered(ObjectNode definition) {
            ObjectNode out = Json.object();
            out.put(RepositoryStore.TYPE, definition.path(RepositoryStore.TYPE).asText(""));
            out.set(RepositoryStore.SETTINGS, Settings.of(definition.path(RepositoryStore.SETTINGS)).toJson(false));
            return out;
        }
    }

    /**
     * {@code DELETE /_snapshot/{repository}}.
     *
     * <p>A single glob rather than a comma-separated list, and {@code *} matching nothing is a 200 while
     * {@code foo*} matching nothing is a 404 — match-all exempt, partial wildcards not, exactly as with
     * deleting an ingest pipeline. See {@link RepositoryStore#resolveForDelete}.
     */
    static final class Delete implements OperationHandler {

        @Override
        public String operationGroup() {
            return "snapshot.delete_repository";
        }

        @Override
        public RestResponse handle(RestRequest request, OperationContext context) {
            String expression = request.pathParam("repository");
            RepositoryStore repositories = new RepositoryStore(context.store());

            RepositoryStore.Resolution resolved = repositories.resolveForDelete(expression);
            if (resolved.failed()) {
                return ErrorResponses.repositoryMissing(request, resolved.missing());
            }
            resolved.found().keySet().forEach(repositories::delete);
            return Json.acknowledged(request);
        }
    }
}
