/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.handler.cat;

import com.fasterxml.jackson.databind.JsonNode;
import org.opensearch.controlplane.handler.OperationContext;
import org.opensearch.controlplane.handler.cluster.ClusterRequests;
import org.opensearch.controlplane.handler.cluster.StatusProjections;
import org.opensearch.controlplane.handler.cluster.StatusProjections.IndexHealth;
import org.opensearch.controlplane.resolver.IndicesOptions;
import org.opensearch.controlplane.rest.ErrorResponses;
import org.opensearch.controlplane.rest.RestRequest;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Locale;
import java.util.Optional;

/**
 * {@code GET /_cat/indices[/{index}]}: one row per index, from this service's index documents and the
 * supervisor's health for them.
 *
 * <p>The document and store-size columns are {@code x}: nothing yet reports them per copy, and the sum of an
 * index's primaries is not even well defined when a rollout runs two primaries of a shard at once. A closed
 * index leaves them blank, as upstream does for one it holds no statistics for. See the data plane's
 * {@code FINDINGS_FROM_THE_CONTROL_PLANE.md}, which asks for them.
 *
 * <p>Rows are the indices the expression names, hidden ones excluded unless {@code expand_wildcards} says
 * otherwise, and a concrete name that does not exist is a {@code 404}. {@code health} keeps only rows of that
 * health. Rows come out sorted by index name, where upstream's order is a hash map's.
 *
 * <p>Not supported, and silently unknown as any unrecognised column is: the {@code pri.}-paired statistics
 * columns, {@code search.*}, {@code last_index_request_timestamp} and {@code system}; nor does {@code pri=true}
 * add the paired column to an {@code h} list.
 */
public final class CatIndicesHandler extends CatHandler {

    private static final List<Column> COLUMNS = List.of(
        Column.left("health", "h", "current health status"),
        Column.left("status", "s", "open/close status"),
        Column.left("index", "i,idx", "index name"),
        Column.left("uuid", "id,uuid", "index uuid"),
        Column.right("pri", "p,shards.primary,shardsPrimary", "number of primary shards"),
        Column.right("rep", "r,shards.replica,shardsReplica", "number of replica shards"),
        Column.right("docs.count", "dc,docsCount", "available docs"),
        Column.right("docs.deleted", "dd,docsDeleted", "deleted docs"),
        Column.left("creation.date", "cd", "index creation date (millisecond value)").optional(),
        Column.left("creation.date.string", "cds", "index creation date (as string)").optional(),
        Column.right("store.size", "ss,storeSize", "store size of primaries & replicas"),
        Column.right("pri.store.size", "", "store size of primaries")
    );

    /** {@code strictExpand}: open and closed, hidden left out, and a missing name is an error. */
    private static final IndicesOptions STRICT_EXPAND = new IndicesOptions(true, true, false, false, true);

    /** {@code strict_date_time}. */
    private static final DateTimeFormatter STRICT_DATE_TIME =
        DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", Locale.ROOT).withZone(ZoneOffset.UTC);

    public CatIndicesHandler() {
        this(ClusterRequests.POLL);
    }

    CatIndicesHandler(Duration poll) {
        super(poll);
    }

    @Override
    public String operationGroup() {
        return "cat.indices";
    }

    @Override
    List<Column> columns() {
        return COLUMNS;
    }

    @Override
    Outcome table(RestRequest request, OperationContext context) {
        Optional<ClusterRequests.Read> read = awaitSupervisor(request, context);
        if (read.isEmpty()) {
            return Outcome.failure(ErrorResponses.clusterManagerNotDiscovered(request));
        }
        StatusProjections projections = read.get().projections();

        Selected selected = resolve(request, context, STRICT_EXPAND);
        if (selected.failure() != null) {
            return Outcome.failure(selected.failure());
        }

        String health = request.param("health");
        CatTable table = new CatTable(COLUMNS);
        for (String name : selected.names()) {
            Optional<IndexHealth> index = projections.index(name);
            if (index.isEmpty()) {
                continue;
            }
            if (health != null && matchesHealth(health, index.get()) == false) {
                continue;
            }
            JsonNode document = index.get().document();
            boolean closed = "close".equals(document.path("state").asText("open"));
            // Statistics exist only for an open index; a closed one has none, which is blank rather than unknown.
            String unknown = closed ? null : CatTable.UNKNOWN;
            Long created = creationDate(document);
            table.row(
                index.get().status().wire(),
                closed ? "close" : "open",
                name,
                document.path("uuid").asText(),
                index.get().numberOfShards(),
                index.get().numberOfReplicas(),
                unknown,
                unknown,
                created,
                created == null ? null : STRICT_DATE_TIME.format(Instant.ofEpochMilli(created)),
                unknown,
                unknown
            );
        }
        return Outcome.of(table);
    }

    /** {@code ClusterHealthStatus.fromString}, case-insensitive, and its error for anything else. */
    private static boolean matchesHealth(String wanted, IndexHealth index) {
        String lower = wanted.toLowerCase(Locale.ROOT);
        if (List.of("green", "yellow", "red").contains(lower) == false) {
            throw new IllegalArgumentException("unknown cluster health status [" + wanted + "]");
        }
        return index.status().wire().equals(lower);
    }

    private static Long creationDate(JsonNode document) {
        String value = document.path("creation_date").asText(null);
        if (value == null) {
            return null;
        }
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException e) {
            return null;
        }
    }
}
