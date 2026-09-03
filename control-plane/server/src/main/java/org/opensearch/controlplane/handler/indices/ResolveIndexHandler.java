/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.handler.indices;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.opensearch.controlplane.handler.OperationContext;
import org.opensearch.controlplane.handler.OperationHandler;
import org.opensearch.controlplane.resolver.IndexAbstraction;
import org.opensearch.controlplane.resolver.IndexAbstractions;
import org.opensearch.controlplane.resolver.IndicesOptions;
import org.opensearch.controlplane.rest.Json;
import org.opensearch.controlplane.rest.NameExpression;
import org.opensearch.controlplane.rest.RestRequest;
import org.opensearch.controlplane.rest.RestResponse;
import org.opensearch.controlplane.rest.RestStatus;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Pattern;

/**
 * {@code GET /_resolve/index/{name}} — what a name refers to, sorted into indices, aliases and data
 * streams.
 *
 * <pre>
 * {"indices":     [{"name": "i", "aliases": ["a"], "attributes": ["open"]}],
 *  "aliases":     [{"name": "a", "indices": ["i"]}],
 *  "data_streams": []}
 * </pre>
 *
 * <h2>Why this does not use the expression resolver</h2>
 *
 * <p>It looks like every other index expression and behaves differently in three ways, all verified
 * against OpenSearch 3.9.0. This operation reports on <em>names</em>; it does not resolve them to
 * concrete indices, which is what the resolver exists to do.
 *
 * <ul>
 *   <li><b>An alias stays an alias.</b> {@code /_resolve/index/a} reports {@code a} under
 *       {@code aliases} with an empty {@code indices} list — where every other endpoint would flatten
 *       it to its member indices.</li>
 *   <li><b>{@code _all} is not special.</b> It is matched as a literal name, matches nothing, and
 *       answers three empty arrays. Everywhere else {@code _all} means everything, and a leading
 *       underscore is an {@code invalid_index_name_exception}.</li>
 *   <li><b>Nothing is ever an error.</b> A name that matches nothing is a {@code 200} with three empty
 *       arrays, not a 404. There is no {@code ignore_unavailable} or {@code allow_no_indices} here —
 *       {@code expand_wildcards} is the only parameter the spec declares.</li>
 * </ul>
 *
 * <p>What is shared is hidden-index expansion: a hidden index stays out of a wildcard match unless
 * {@code expand_wildcards} includes hidden, or the pattern is itself dot-prefixed. Verified —
 * {@code /_resolve/index/*} omits {@code .hidden} while naming it outright finds it.
 *
 * <p>{@code data_streams} is always empty: the namespace is out of scope, so there are none to report.
 * The key is present because a client parsing the response should not have to handle its absence.
 */
public final class ResolveIndexHandler implements OperationHandler {

    @Override
    public String operationGroup() {
        return "indices.resolve_index";
    }

    @Override
    public RestResponse handle(RestRequest request, OperationContext context) {
        String expression = request.pathParam("name");
        List<Pattern> patterns = NameExpression.parse(expression);
        // Only the hidden bit of expand_wildcards matters here: there is nothing to expand open or
        // closed against, since names are matched rather than resolved.
        boolean expandHidden = IndicesOptions.strictExpandOpen().withOverridesFrom(request).expandHidden()
            || startsWithDot(expression);

        IndexAbstractions abstractions = IndexAbstractions.from(context.store());

        // index name -> its aliases, built by inverting the alias abstractions rather than rescanning.
        Map<String, TreeSet<String>> aliasesByIndex = new TreeMap<>();
        abstractions.all().values().stream()
            .filter(IndexAbstraction::alias)
            .forEach(alias -> alias.indices()
                .forEach(index -> aliasesByIndex.computeIfAbsent(index, ignored -> new TreeSet<>()).add(alias.name())));

        ArrayNode indices = Json.array();
        ArrayNode aliases = Json.array();

        for (IndexAbstraction abstraction : abstractions.all().values()) {
            if (NameExpression.matchesAny(patterns, abstraction.name()) == false) {
                continue;
            }
            if (abstraction.hidden() && expandHidden == false) {
                continue;
            }
            if (abstraction.alias()) {
                ObjectNode entry = Json.object();
                entry.put("name", abstraction.name());
                ArrayNode members = Json.array();
                new TreeSet<>(abstraction.indices()).forEach(members::add);
                entry.set("indices", members);
                aliases.add(entry);
            } else {
                indices.add(describeIndex(abstraction, aliasesByIndex.get(abstraction.name())));
            }
        }

        ObjectNode body = Json.object();
        body.set("indices", indices);
        body.set("aliases", aliases);
        body.set("data_streams", Json.array());
        return Json.response(RestStatus.OK, body, request);
    }

    /**
     * {@code name}, then {@code aliases} only when there are some, then {@code attributes}.
     *
     * <p>The omission is not tidiness: an index with no aliases has no {@code aliases} key at all on a
     * real cluster, while {@code attributes} is always present. Verified.
     */
    private static ObjectNode describeIndex(IndexAbstraction index, TreeSet<String> aliases) {
        ObjectNode entry = Json.object();
        entry.put("name", index.name());
        if (aliases != null && aliases.isEmpty() == false) {
            ArrayNode names = Json.array();
            aliases.forEach(names::add);
            entry.set("aliases", names);
        }
        ArrayNode attributes = Json.array();
        // Verified ordering: a hidden open index reports ["hidden", "open"].
        if (index.hidden()) {
            attributes.add("hidden");
        }
        attributes.add(index.open() ? "open" : "closed");
        entry.set("attributes", attributes);
        return entry;
    }

    /** A dot-prefixed pattern reaches dot-prefixed hidden indices, as it does in the resolver. */
    private static boolean startsWithDot(String expression) {
        if (expression == null) {
            return false;
        }
        List<String> tokens = new ArrayList<>(List.of(expression.split(",")));
        return tokens.stream().map(String::trim).anyMatch(token -> token.startsWith("."));
    }
}
