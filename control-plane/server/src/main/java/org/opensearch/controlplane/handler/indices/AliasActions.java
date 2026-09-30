/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.handler.indices;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.opensearch.controlplane.resolver.IndexAbstractions;
import org.opensearch.controlplane.resolver.IndexNameExpressionResolver;
import org.opensearch.controlplane.resolver.IndexResolution;
import org.opensearch.controlplane.resolver.IndicesOptions;
import org.opensearch.controlplane.rest.ErrorResponses;
import org.opensearch.controlplane.rest.Json;
import org.opensearch.controlplane.rest.RestRequest;
import org.opensearch.controlplane.rest.RestResponse;
import org.opensearch.controlplane.store.KeyValueStore;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;

/**
 * The alias mutation engine behind {@code _aliases}, {@code put_alias} and {@code delete_alias}.
 *
 * <p>All three endpoints are the same operation: OpenSearch implements {@code put_alias} as a single
 * {@code add} action and {@code delete_alias} as a single {@code remove}, so the logic lives here once
 * and the handlers differ only in how they build their action list.
 *
 * <p>An action names indices as {@code index} or {@code indices} — wildcards allowed, resolved through
 * {@link IndexNameExpressionResolver} — and aliases as {@code alias} or {@code aliases}.
 *
 * <h2>Validation, all verified against OpenSearch 3.9.0</h2>
 *
 * <ul>
 *   <li>An empty action list is {@code 400 illegal_argument_exception: No action specified}.</li>
 *   <li>Adding an alias whose name is already an index is {@code 400 invalid_alias_name_exception}.</li>
 *   <li>Naming an index that does not exist is {@code 404 index_not_found_exception}.</li>
 *   <li>Removing an alias that is not there is {@code 404 aliases_not_found_exception}, unless the
 *       action sets {@code must_exist: false}.</li>
 *   <li>Two write indices for one alias is rejected — see
 *       {@link ErrorResponses#multipleWriteIndices}, which is a considered divergence on status.</li>
 * </ul>
 *
 * <h2>Atomicity</h2>
 *
 * <p>Actions are validated in full before anything is written, so a request that cannot succeed writes
 * nothing. Each alias is then written as one document, which makes the canonical swap — remove from one
 * index, add to another, same alias — genuinely atomic, since it touches a single key.
 *
 * <p>Each alias is written by compare-and-set on the revision it was read at, and losing that race is a
 * 409 rather than a silently lost update. With several aliases in one request, the ones before the
 * conflict have already been written.
 *
 * <p>A request spanning <em>several</em> aliases is still several writes and so not atomic as a whole.
 * That is deliberate rather than overlooked: the shard manager assembles what it pushes to coordinator
 * nodes atomically, and aliases matter for dispatch, so a brief disagreement between our own keys is not
 * observable by anything routing requests. Avoiding it here would need multi-key transactions, which
 * would rule out object-store backends.
 */
final class AliasActions {

    /** One parsed action. Exactly one of the three kinds, per the spec's {@code maxProperties: 1}. */
    private record Action(Kind kind, List<String> indices, List<String> aliases, ObjectNode properties, boolean mustExist) {
        enum Kind { ADD, REMOVE, REMOVE_INDEX }
    }

    private static final Set<String> PROPERTY_FIELDS =
        Set.of("filter", "index_routing", "search_routing", "routing", "is_write_index", "is_hidden");

    private final KeyValueStore store;
    private final AliasStore aliases;
    private final IndexStore indices;

    AliasActions(KeyValueStore store) {
        this.store = store;
        this.aliases = new AliasStore(store);
        this.indices = new IndexStore(store);
    }

    /**
     * Applies an action list.
     *
     * @return the error response if anything was rejected; empty when applied
     */
    Optional<RestResponse> apply(RestRequest request, List<ObjectNode> rawActions) {
        if (rawActions.isEmpty()) {
            return Optional.of(ErrorResponses.noActionSpecified(request));
        }

        IndexAbstractions abstractions = IndexAbstractions.from(store);
        IndexNameExpressionResolver resolver = new IndexNameExpressionResolver(abstractions);

        // Staged, not applied: nothing is written until every action has been checked.
        Map<String, AliasStore.Versioned> staged = new TreeMap<>();
        Set<String> indicesToRemove = new LinkedHashSet<>();

        for (ObjectNode raw : rawActions) {
            Action action;
            try {
                action = parse(raw);
            } catch (IllegalArgumentException e) {
                return Optional.of(ErrorResponses.structured(
                    org.opensearch.controlplane.rest.RestStatus.BAD_REQUEST,
                    "illegal_argument_exception",
                    e.getMessage(),
                    request
                ));
            }

            List<String> resolved = new ArrayList<>();
            for (String expression : action.indices()) {
                IndexResolution resolution = resolver.resolve(expression, IndicesOptions.strictExpandOpen());
                switch (resolution) {
                    case IndexResolution.Resolved r -> resolved.addAll(r.indices());
                    case IndexResolution.NotFound nf -> {
                        return Optional.of(ErrorResponses.indexNotFound(request, nf.expression()));
                    }
                    case IndexResolution.InvalidName in -> {
                        return Optional.of(ErrorResponses.invalidIndexName(request, in.expression(), in.reason()));
                    }
                }
            }

            if (action.kind() == Action.Kind.REMOVE_INDEX) {
                indicesToRemove.addAll(resolved);
                continue;
            }

            for (String alias : action.aliases()) {
                if (action.kind() == Action.Kind.ADD) {
                    Optional<RestResponse> rejected = rejectIfIndexNameCollision(request, indices, alias);
                    if (rejected.isPresent()) {
                        return rejected;
                    }
                }
                ObjectNode members = (ObjectNode) staged.computeIfAbsent(alias, aliases::getVersioned)
                    .definition().get(AliasStore.INDICES);

                if (action.kind() == Action.Kind.ADD) {
                    resolved.forEach(index -> members.set(index, action.properties().deepCopy()));
                } else {
                    Optional<RestResponse> rejected = removeMembers(request, alias, members, resolved, action.mustExist());
                    if (rejected.isPresent()) {
                        return rejected;
                    }
                }
            }
        }

        // Write-index uniqueness is a property of the final state, so it is checked after staging.
        for (Map.Entry<String, AliasStore.Versioned> entry : staged.entrySet()) {
            Optional<RestResponse> rejected =
                rejectIfMultipleWriteIndices(request, entry.getKey(), entry.getValue().definition());
            if (rejected.isPresent()) {
                return rejected;
            }
        }

        for (Map.Entry<String, AliasStore.Versioned> entry : staged.entrySet()) {
            // Not retried: the actions were validated against what was read, and an alias another
            // request has changed since may no longer satisfy them.
            if (aliases.replace(entry.getKey(), entry.getValue().definition(), entry.getValue().revision()) == false) {
                return Optional.of(ErrorResponses.concurrentAliasUpdate(request, entry.getKey()));
            }
        }
        indicesToRemove.forEach(index -> {
            indices.delete(index);
            removeIndexFromAllAliases(index);
        });
        return Optional.empty();
    }

    /**
     * Drops an index from every alias naming it. Used by {@code remove_index} and index deletion.
     *
     * <p>Retried on a lost race rather than reported: dropping one member is still right against
     * whatever the alias holds by then, and the index is already gone.
     */
    void removeIndexFromAllAliases(String index) {
        for (String alias : aliases.forIndex(index).keySet()) {
            AliasStore.Versioned current;
            do {
                current = aliases.getVersioned(alias);
                if (((ObjectNode) current.definition().get(AliasStore.INDICES)).remove(index) == null) {
                    break;
                }
            } while (aliases.replace(alias, current.definition(), current.revision()) == false);
        }
    }

    private Optional<RestResponse> removeMembers(
        RestRequest request,
        String alias,
        ObjectNode members,
        List<String> resolved,
        boolean mustExist
    ) {
        boolean removedAny = false;
        for (String index : resolved) {
            if (members.remove(index) != null) {
                removedAny = true;
            }
        }
        if (removedAny == false && mustExist) {
            return Optional.of(ErrorResponses.aliasesNotFound(request, alias));
        }
        return Optional.empty();
    }

    /** An alias may not share a name with an index. Also checked by {@code indices.create}. */
    static Optional<RestResponse> rejectIfIndexNameCollision(RestRequest request, IndexStore indices, String alias) {
        return indices.get(alias)
            .map(metadata -> ErrorResponses.invalidAliasName(
                request, alias, alias, metadata.path("uuid").asText("_na_")));
    }

    /** At most one member may be the explicit write index. Also checked by {@code indices.create}. */
    static Optional<RestResponse> rejectIfMultipleWriteIndices(RestRequest request, String alias, ObjectNode definition) {
        List<String> writeIndices = new ArrayList<>();
        definition.path(AliasStore.INDICES).properties().forEach(member -> {
            if (member.getValue().path("is_write_index").asBoolean(false)) {
                writeIndices.add(member.getKey());
            }
        });
        return writeIndices.size() > 1
            ? Optional.of(ErrorResponses.multipleWriteIndices(request, alias, writeIndices))
            : Optional.empty();
    }

    /** Parses one action object; the spec allows exactly one of add, remove, remove_index. */
    private static Action parse(ObjectNode raw) {
        Map<String, Action.Kind> kinds = new LinkedHashMap<>();
        kinds.put("add", Action.Kind.ADD);
        kinds.put("remove", Action.Kind.REMOVE);
        kinds.put("remove_index", Action.Kind.REMOVE_INDEX);

        for (Map.Entry<String, Action.Kind> candidate : kinds.entrySet()) {
            JsonNode body = raw.get(candidate.getKey());
            if (body == null) {
                continue;
            }
            if (body.isObject() == false) {
                throw new IllegalArgumentException("[" + candidate.getKey() + "] must be an object");
            }
            List<String> indices = names(body, "index", "indices");
            List<String> aliases = names(body, "alias", "aliases");
            if (indices.isEmpty()) {
                throw new IllegalArgumentException("[" + candidate.getKey() + "] requires an index");
            }
            if (candidate.getValue() != Action.Kind.REMOVE_INDEX && aliases.isEmpty()) {
                throw new IllegalArgumentException("[" + candidate.getKey() + "] requires an alias");
            }

            ObjectNode properties = Json.object();
            PROPERTY_FIELDS.forEach(field -> {
                if (body.has(field)) {
                    properties.set(field, body.get(field).deepCopy());
                }
            });

            return new Action(
                candidate.getValue(),
                indices,
                aliases,
                properties,
                body.path("must_exist").asBoolean(true)
            );
        }
        throw new IllegalArgumentException("No action specified");
    }

    /** Reads a singular-or-plural field: {@code index} or {@code indices}, {@code alias} or {@code aliases}. */
    private static List<String> names(JsonNode body, String singular, String plural) {
        List<String> names = new ArrayList<>();
        JsonNode one = body.get(singular);
        if (one != null && one.isTextual()) {
            names.add(one.asText());
        }
        JsonNode many = body.get(plural);
        if (many != null) {
            if (many.isArray()) {
                many.forEach(value -> names.add(value.asText()));
            } else if (many.isTextual()) {
                names.add(many.asText());
            }
        }
        return names;
    }
}
