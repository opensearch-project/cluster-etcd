/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.handler.cat;

import org.opensearch.controlplane.handler.OperationContext;
import org.opensearch.controlplane.handler.OperationHandler;
import org.opensearch.controlplane.handler.cluster.ClusterRequests;
import org.opensearch.controlplane.resolver.IndexAbstractions;
import org.opensearch.controlplane.resolver.IndexNameExpressionResolver;
import org.opensearch.controlplane.resolver.IndexResolution;
import org.opensearch.controlplane.resolver.IndicesOptions;
import org.opensearch.controlplane.rest.ErrorResponses;
import org.opensearch.controlplane.rest.RestRequest;
import org.opensearch.controlplane.rest.RestResponse;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

/**
 * The shape every cat operation shares: build a {@link CatTable}, then let {@link CatRendering} show it the way
 * the client asked. A subclass supplies the columns and the rows.
 *
 * <p>All four tables are views of what the supervisor projects, so all four wait, as {@code cluster.health}
 * does, for a fresh {@code status/cluster} and otherwise answer {@code 503 cluster_manager_not_discovered_exception}
 * after {@code cluster_manager_timeout}; {@code local=true} takes the last documents instead. {@code ?help} lists
 * the columns without reading anything.
 */
abstract class CatHandler implements OperationHandler {

    private final Duration poll;

    CatHandler(Duration poll) {
        this.poll = poll;
    }

    /** Every column the table has, defaults and optional ones, for {@code ?help} and for resolving {@code h}. */
    abstract List<Column> columns();

    /** The table, or the response to send instead of one. */
    abstract Outcome table(RestRequest request, OperationContext context);

    /** A table, or why there is not one. */
    record Outcome(CatTable table, RestResponse failure) {
        static Outcome of(CatTable table) {
            return new Outcome(table, null);
        }

        static Outcome failure(RestResponse response) {
            return new Outcome(null, response);
        }
    }

    @Override
    public final RestResponse handle(RestRequest request, OperationContext context) {
        if (request.paramAsBoolean("help", false)) {
            return CatRendering.help(columns());
        }
        Optional<RestResponse> both = ClusterRequests.rejectBothManagerTimeouts(request);
        if (both.isPresent()) {
            return both.get();
        }
        try {
            Outcome outcome = table(request, context);
            return outcome.failure() != null ? outcome.failure() : CatRendering.render(request, outcome.table());
        } catch (IllegalArgumentException e) {
            return ErrorResponses.illegalArgument(request, e.getMessage());
        }
    }

    /** The supervisor's projections once a fresh one is there, or empty if none appeared in time. */
    final Optional<ClusterRequests.Read> awaitSupervisor(RestRequest request, OperationContext context) {
        Duration timeout = ClusterRequests.timeValue(
            request, ClusterRequests.clusterManagerTimeoutParam(request), ClusterRequests.DEFAULT_CLUSTER_MANAGER_TIMEOUT);
        return ClusterRequests.awaitFresh(context, timeout, request.paramAsBoolean("local", false), poll);
    }

    /** The indices an expression names, or the 404 or 400 for one that cannot be resolved. */
    record Selected(List<String> names, RestResponse failure) {}

    /**
     * Resolves the {@code {index}} path expression, a comma-separated list that may hold wildcards, under
     * {@code defaults} as overridden by {@code expand_wildcards}, {@code ignore_unavailable} and
     * {@code allow_no_indices}. No expression is every index the options allow.
     */
    static Selected resolve(RestRequest request, OperationContext context, IndicesOptions defaults) {
        String expression = request.pathParam("index");
        List<String> expressions = expression == null || expression.isEmpty()
            ? List.of("*")
            : Arrays.stream(expression.split(",")).filter(name -> name.isEmpty() == false).toList();
        IndexResolution resolution = new IndexNameExpressionResolver(IndexAbstractions.from(context.store()))
            .resolve(expressions, defaults.withOverridesFrom(request));
        return switch (resolution) {
            case IndexResolution.Resolved resolved ->
                new Selected(resolved.indices().stream().distinct().sorted().toList(), null);
            case IndexResolution.NotFound notFound ->
                new Selected(List.of(), ErrorResponses.indexNotFound(request, notFound.expression()));
            case IndexResolution.InvalidName invalid ->
                new Selected(List.of(), ErrorResponses.invalidIndexName(request, invalid.expression(), invalid.reason()));
        };
    }
}
