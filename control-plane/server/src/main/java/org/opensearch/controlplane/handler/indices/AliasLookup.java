/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.handler.indices;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.opensearch.controlplane.resolver.IndexAbstractions;
import org.opensearch.controlplane.resolver.IndexNameExpressionResolver;
import org.opensearch.controlplane.resolver.IndexResolution;
import org.opensearch.controlplane.resolver.IndicesOptions;
import org.opensearch.controlplane.rest.Json;
import org.opensearch.controlplane.rest.NameExpression;
import org.opensearch.controlplane.rest.RestRequest;

import java.util.List;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.regex.Pattern;

/**
 * The shared read path behind {@code get_alias} and {@code exists_alias}.
 *
 * <p>Both answer the same question — which indices carry which of the requested aliases — and differ
 * only in whether they render it. Kept in one place so their notion of "found" cannot drift.
 *
 * <h2>The rule that shapes everything</h2>
 *
 * <p>Responses are <b>index-keyed</b> even when the request names an alias:
 * {@code GET /_alias/x} answers {@code {"index": {"aliases": {"x": {}}}}}. So these endpoints need
 * index→aliases, which is a scan, regardless of how aliases are stored. Verified against
 * OpenSearch 3.9.0.
 *
 * <h2>Whether an alias name was given changes the outcome</h2>
 *
 * <p>Not a detail — it decides both the membership and the status code:
 *
 * <ul>
 *   <li><b>No alias name</b> ({@code GET /_alias}, {@code GET /{index}/_alias}) — every matching index
 *       appears, including those with no aliases at all, rendered as {@code "aliases": {}}. Always 200.
 *       Verified: {@code GET /_alias} on a cluster where only one of two indices has an alias returns
 *       both.</li>
 *   <li><b>An alias name or pattern</b> ({@code GET /_alias/x}) — only indices carrying a match appear,
 *       and nothing matching is a 404. Verified: {@code GET /cp-a/_alias/cp-x} is a 404 when
 *       {@code cp-x} is on {@code cp-b}, even though both the index and the alias exist.</li>
 * </ul>
 */
final class AliasLookup {

    /** What the request asked for, resolved. */
    record Result(NavigableMap<String, ObjectNode> byIndex, boolean anyAliasMatched) {}

    private AliasLookup() {}

    /**
     * @param indexExpression the {@code {index}} placeholder, or {@code null} for every index
     * @param aliasExpression the {@code {name}} placeholder, or {@code null} for "no filter"
     * @return the index-keyed result, or empty when the index expression itself could not be resolved
     */
    static Optional<Result> lookup(
        RestRequest request,
        org.opensearch.controlplane.store.KeyValueStore store,
        String indexExpression,
        String aliasExpression
    ) {
        IndicesOptions options = IndicesOptions.lenient().withOverridesFrom(request);
        IndexResolution resolution =
            new IndexNameExpressionResolver(IndexAbstractions.from(store)).resolve(indexExpression, options);
        if (resolution instanceof IndexResolution.Resolved resolved) {
            return Optional.of(collect(request, store, resolved.indices(), aliasExpression));
        }
        return Optional.empty();
    }

    private static Result collect(
        RestRequest request,
        org.opensearch.controlplane.store.KeyValueStore store,
        List<String> indices,
        String aliasExpression
    ) {
        AliasStore aliases = new AliasStore(store);
        List<Pattern> patterns = aliasExpression == null ? null : NameExpression.parse(aliasExpression);

        NavigableMap<String, ObjectNode> byIndex = new java.util.TreeMap<>();
        boolean anyMatched = false;

        for (String index : indices) {
            ObjectNode aliasesForIndex = Json.object();
            for (var entry : aliases.forIndex(index).entrySet()) {
                if (patterns == null || NameExpression.matchesAny(patterns, entry.getKey())) {
                    aliasesForIndex.set(entry.getKey(), entry.getValue().deepCopy());
                    anyMatched = true;
                }
            }
            // With no alias filter, an index with no aliases is still reported, as "aliases": {}.
            if (patterns == null || aliasesForIndex.isEmpty() == false) {
                ObjectNode wrapper = Json.object();
                wrapper.set("aliases", aliasesForIndex);
                byIndex.put(index, wrapper);
            }
        }
        return new Result(byIndex, anyMatched);
    }
}
