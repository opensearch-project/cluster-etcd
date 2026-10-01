/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.handler.indices;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.opensearch.controlplane.handler.OperationContext;
import org.opensearch.controlplane.handler.OperationHandler;
import org.opensearch.controlplane.handler.template.TemplateComposition;
import org.opensearch.controlplane.rest.ErrorResponses;
import org.opensearch.controlplane.rest.Json;
import org.opensearch.controlplane.rest.NameExpression;
import org.opensearch.controlplane.rest.RestRequest;
import org.opensearch.controlplane.rest.RestResponse;

import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;

/**
 * Data streams: {@code PUT} and {@code DELETE} on {@code /_data_stream/{name}}.
 *
 * <p>A data stream is a write-time abstraction over a series of hidden backing indices. Creating one is
 * metadata work that finishes in its own request — find the template, create the first backing index,
 * write the stream document — which is what puts it here.
 *
 * <h2>The composable template is a hard prerequisite</h2>
 *
 * <p>Unlike an index, a data stream <b>cannot</b> be created without a matching composable index template
 * carrying a {@code data_stream} block. The template supplies the timestamp field name, and its absence is
 * an error rather than a set of defaults. This is why data streams landed after template CRUD rather than
 * before: {@link TemplateComposition#matchingIndexTemplate} is {@code findV2Template}, and without it
 * there is nothing to look up.
 *
 * <p><b>Templates are matched against the stream name, not the backing index name.</b> Easy to get
 * backwards and it fails silently if you do — the backing index would match no template and quietly get
 * built-in defaults. Verified: a template on {@code cp-probe-ds*} with {@code number_of_shards: 1}
 * produced {@code .ds-cp-probe-ds1-000001} with one shard, and {@code .ds-...} matches that pattern
 * nowhere.
 *
 * <h2>Verified behaviour</h2>
 *
 * <ul>
 *   <li><b>Create is not an upsert</b>, unlike templates and pipelines: a second {@code PUT} of the same
 *       name is {@code 400 resource_already_exists_exception}, {@code data_stream [x] already exists}. The
 *       existence check runs <em>before</em> name validation, so an existing stream with an otherwise
 *       illegal name still reports already-exists.</li>
 *   <li><b>Name rules are the index rules plus one.</b> The index reasons are reused verbatim but
 *       rewrapped — {@code data_stream [X] must be lowercase}, an {@code illegal_argument_exception}
 *       rather than the {@code invalid_index_name_exception} the index path gives. The extra rule is that a
 *       data stream <b>may not start with a dot</b>, where an index may; it is checked after the shared
 *       rules, so {@code ..} reports {@code must not be '.' or '..'} and {@code .x} reports
 *       {@code must not start with '.'}.</li>
 *   <li><b>{@code DELETE} never reports a miss.</b> Every name is treated as a glob and matched against
 *       the streams that exist, with no not-found check anywhere — {@code resolveDataStreams} is a bare
 *       {@code Regex.simpleMatch} loop. So deleting a concrete name that does not exist is {@code 200},
 *       and so is a wildcard matching nothing. This is <em>not</em> the metadata-presence quirk that
 *       governs pipelines and repositories; it is unconditional.</li>
 *   <li><b>{@code DELETE} removes the backing indices too</b>, which is the one place an index disappears
 *       without {@code indices.delete} being called.</li>
 *   <li>The first backing index is {@code .ds-<name>-000001} and carries {@code index.hidden: true} on top
 *       of whatever the template composed.</li>
 * </ul>
 *
 * <h2>Not implemented here</h2>
 *
 * <p>{@code indices.get_data_stream} is deliberately still stubbed: its {@code status} field is a
 * {@code HealthStatus} over the backing indices and the spec marks it required, so the response cannot be
 * built honestly without the data-plane health this service does not have. Same dependency that keeps
 * {@code cluster.health} stubbed. Creating and deleting need no such thing.
 *
 * <p>{@code rollover} is what advances the generation, and it is deferred — so a stream created here stays
 * at generation 1 with one backing index. That is a real limit, not an oversight: the stored
 * {@code generation} counter is there so rollover has something to increment.
 */
public final class DataStreamHandlers {

    private DataStreamHandlers() {}

    public static List<OperationHandler> handlers() {
        return List.of(new Create(), new Delete());
    }

    /** {@code PUT /_data_stream/{name}}. */
    static final class Create implements OperationHandler {

        @Override
        public String operationGroup() {
            return "indices.create_data_stream";
        }

        @Override
        public RestResponse handle(RestRequest request, OperationContext context) {
            String name = request.pathParam("name");
            DataStreamStore streams = new DataStreamStore(context.store());

            // Before name validation, which is the order MetadataCreateDataStreamService uses.
            if (streams.exists(name)) {
                return ErrorResponses.dataStreamAlreadyExists(request, name);
            }

            Optional<String> invalid = IndexNameValidator.validate(name);
            if (invalid.isPresent()) {
                // The index reasons, rewrapped. "index name is too long" reads oddly under a data_stream
                // prefix; it is what the server says, because the message comes from the shared validator.
                return ErrorResponses.illegalArgument(request, "data_stream [" + name + "] " + invalid.get());
            }
            if (name.startsWith(".")) {
                return ErrorResponses.illegalArgument(request, "data_stream [" + name + "] must not start with '.'");
            }

            Optional<TemplateComposition.Match> match =
                TemplateComposition.matchingIndexTemplate(context.store(), name);
            if (match.isEmpty()) {
                return ErrorResponses.illegalArgument(
                    request, "no matching index template found for data stream [" + name + "]");
            }
            if (match.get().template().has(DATA_STREAM) == false) {
                return ErrorResponses.illegalArgument(
                    request,
                    "matching index template [" + match.get().name() + "] for data stream [" + name
                        + "] has no data stream template"
                );
            }

            String timestampField = match.get().template()
                .path(DATA_STREAM).path(DataStreamStore.TIMESTAMP_FIELD).path(DataStreamStore.NAME)
                .asText(DataStreamStore.DEFAULT_TIMESTAMP_FIELD);

            // Composed against the stream name, written under the backing index name. See the class javadoc.
            TemplateComposition.Composed composed =
                TemplateComposition.composeFor(context.store(), name, Json.object());
            composed = new TemplateComposition.Composed(
                composed.settings().with(SETTING_HIDDEN, "true"), composed.mappings(), composed.aliases());

            String backingIndex = DataStreamStore.backingIndexName(name, FIRST_GENERATION);
            // The backing index is created as an ordinary index upstream, so its name is validated as one: a
            // stream name within 11 bytes of the limit passes above and fails here, on the .ds- name.
            Optional<String> invalidBacking = IndexNameValidator.validate(backingIndex);
            if (invalidBacking.isPresent()) {
                return ErrorResponses.invalidIndexName(request, backingIndex, invalidBacking.get());
            }
            IndexStore indices = new IndexStore(context.store());
            String uuid = IndexStore.newUuid();
            AliasStore aliases = new AliasStore(context.store());
            Optional<RestResponse> aliasRejection =
                CreateIndexHandler.checkAliases(request, indices, aliases, backingIndex, uuid, composed.aliases());
            if (aliasRejection.isPresent()) {
                return aliasRejection.get();
            }
            ObjectNode metadata =
                CreateIndexHandler.buildMetadata(backingIndex, uuid, composed, context.nowEpochMillis());
            Optional<RestResponse> tooLarge = IndexStore.rejectIfTooLarge(request, backingIndex, metadata);
            if (tooLarge.isPresent()) {
                return tooLarge.get();
            }

            if (indices.createIfAbsent(metadata, backingIndex).isEmpty()) {
                return ErrorResponses.illegalArgument(
                    request,
                    "data stream could not be created because backing index [" + backingIndex + "] already exists"
                );
            }

            if (streams.createIfAbsent(
                name,
                timestampField,
                FIRST_GENERATION,
                List.of(new DataStreamStore.BackingIndex(backingIndex, uuid))
            ) == false) {
                // Another request created the stream between the existence check and here. Its backing index
                // write would have lost the create-if-absent above, so reaching this means the two raced on
                // different keys; the index we just wrote is now an orphan a reconciliation pass can find.
                return ErrorResponses.dataStreamAlreadyExists(request, name);
            }

            // A backing index inherits the template's aliases, verified against 3.9.0.
            aliasRejection = CreateIndexHandler.applyAliases(request, indices, aliases, backingIndex, composed.aliases());
            if (aliasRejection.isPresent()) {
                // An alias changed since checkAliases passed. The stream and its backing index are left for
                // reconciliation, as when the stream itself loses a race above.
                return aliasRejection.get();
            }

            new IndexManagementTasks(context.store()).submit(
                IndexManagementTasks.TYPE_CREATE_INDEX,
                backingIndex,
                uuid,
                context.nowEpochMillis()
            );
            return Json.acknowledged(request);
        }
    }

    /**
     * {@code DELETE /_data_stream/{name}}.
     *
     * <p>Globs all the way down, and never a 404 — see the class javadoc.
     */
    static final class Delete implements OperationHandler {

        @Override
        public String operationGroup() {
            return "indices.delete_data_stream";
        }

        @Override
        public RestResponse handle(RestRequest request, OperationContext context) {
            List<Pattern> patterns = NameExpression.parse(request.pathParam("name"));
            DataStreamStore streams = new DataStreamStore(context.store());
            IndexStore indices = new IndexStore(context.store());
            AliasActions aliases = new AliasActions(context.store());
            IndexManagementTasks tasks = new IndexManagementTasks(context.store());

            streams.all().forEach((name, document) -> {
                if (NameExpression.matchesAny(patterns, name) == false) {
                    return;
                }
                // The stream first, then its indices, so a failure between the two leaves orphaned indices
                // rather than a stream pointing at indices that are gone. Same order OpenSearch uses, and
                // for the same reason -- a dangling reference is the worse of the two.
                streams.delete(name);
                // Each backing index goes exactly as indices.delete removes one: its alias memberships too,
                // since a backing index inherits its template's aliases, and a delete-index task, which is
                // what tells the supervisor the index was deleted rather than merely not found.
                DataStreamStore.backingIndicesOf(document).forEach(backing -> {
                    if (indices.delete(backing.name())) {
                        aliases.removeIndexFromAllAliases(backing.name());
                        tasks.submit(IndexManagementTasks.TYPE_DELETE_INDEX, backing.name(), backing.uuid(),
                            context.nowEpochMillis());
                    }
                });
            });
            return Json.acknowledged(request);
        }
    }

    /** The template block that makes a composable template a data stream template. */
    private static final String DATA_STREAM = "data_stream";

    private static final String SETTING_HIDDEN = "index.hidden";

    private static final long FIRST_GENERATION = 1;
}
