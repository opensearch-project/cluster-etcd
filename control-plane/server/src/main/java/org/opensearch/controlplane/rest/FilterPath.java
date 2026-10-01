/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.rest;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * The {@code filter_path} response filter.
 *
 * <p>Semantics verified against OpenSearch 3.9.0:
 *
 * <ul>
 *   <li>Comma-separated paths, dot-separated segments: {@code version.number} yields
 *       {@code {"version":{"number":...}}}, keeping the nesting rather than flattening.</li>
 *   <li>{@code *} matches exactly one segment; {@code nodes.*.name} reaches into a map keyed by node
 *       id. {@code **} matches zero or more.</li>
 *   <li>A {@code -} prefix excludes. With only exclusions, everything else is kept; with any
 *       inclusion, only matches are kept.</li>
 *   <li><b>Arrays are transparent.</b> An array index is not a path segment, so
 *       {@code filter_path=index} over {@code [{"index":"a","health":"green"}]} gives
 *       {@code [{"index":"a"}]}, and {@code *.index} matches nothing. This is the detail most likely
 *       to be guessed wrong.</li>
 *   <li>Nothing matching yields {@code {}} rather than an error.</li>
 * </ul>
 *
 * <p>Not applied to error responses — a real cluster ignores {@code filter_path} when rendering an
 * exception, so {@code GET /nope?filter_path=status} returns the whole error body. Reproduced by
 * applying this only on the success path.
 */
final class FilterPath {

    private static final String MATCH_ONE = "*";
    private static final String MATCH_MANY = "**";

    /** One filter, and how far through it we have matched. */
    private record Cursor(String[] segments, int index) {

        boolean matches(String field) {
            String segment = segments[index];
            return MATCH_MANY.equals(segment) || MATCH_ONE.equals(segment) || segment.equals(field);
        }

        /** Where this filter stands after consuming {@code field}, possibly in more than one place. */
        List<Cursor> advance(String field) {
            if (matches(field) == false) {
                return List.of();
            }
            List<Cursor> next = new ArrayList<>(2);
            if (index + 1 <= segments.length) {
                next.add(new Cursor(segments, index + 1));
            }
            // ** also matches further segments, so it stays put as well as advancing.
            if (MATCH_MANY.equals(segments[index])) {
                next.add(this);
            }
            return next;
        }

        boolean isSatisfied() {
            return index >= segments.length;
        }
    }

    private FilterPath() {}

    /**
     * @param filterPath the raw parameter value, e.g. {@code version.number,-version.build_type}
     * @return the filtered document, or {@code node} unchanged when {@code filterPath} is blank
     */
    static JsonNode apply(JsonNode node, String filterPath) {
        if (filterPath == null || filterPath.isBlank()) {
            return node;
        }

        List<Cursor> includes = new ArrayList<>();
        List<Cursor> excludes = new ArrayList<>();
        for (String raw : filterPath.split(",")) {
            String filter = raw.trim();
            if (filter.isEmpty()) {
                continue;
            }
            if (filter.startsWith("-")) {
                String body = filter.substring(1);
                if (body.isEmpty() == false) {
                    excludes.add(new Cursor(body.split("\\."), 0));
                }
            } else {
                includes.add(new Cursor(filter.split("\\."), 0));
            }
        }

        if (includes.isEmpty() && excludes.isEmpty()) {
            return node;
        }
        // Only exclusions means "keep everything else".
        JsonNode filtered = filter(node, includes, excludes, includes.isEmpty());
        return filtered == null ? Json.object() : filtered;
    }

    /**
     * @return the filtered node, or {@code null} when nothing here survives. Returning null rather
     *         than an empty node matters for scalars: a path that is still expecting more segments
     *         cannot be satisfied by a leaf, so {@code *.index} over {@code [{"index":"a"}]} has to
     *         drop the value rather than keep it.
     */
    private static JsonNode filter(JsonNode node, List<Cursor> includes, List<Cursor> excludes, boolean includeAll) {
        if (node.isArray()) {
            // Array indices are not path segments: each element is filtered at the same position.
            ArrayNode filtered = Json.array();
            for (JsonNode element : node) {
                JsonNode child = filter(element, includes, excludes, includeAll);
                if (child != null && isEmptyContainer(child) == false) {
                    filtered.add(child);
                }
            }
            return filtered;
        }
        if (node.isObject() == false) {
            // A leaf can only be kept if it was already fully matched by its parent.
            return includeAll ? node : null;
        }

        ObjectNode filtered = Json.object();
        for (Map.Entry<String, JsonNode> field : node.properties()) {
            List<Cursor> nextExcludes = advanceAll(excludes, field.getKey());
            if (anySatisfied(nextExcludes)) {
                continue;
            }

            List<Cursor> nextIncludes = advanceAll(includes, field.getKey());
            if (anySatisfied(nextIncludes) || includeAll) {
                // Wholly included, but exclusions still apply beneath it.
                JsonNode value = nextExcludes.isEmpty()
                    ? field.getValue()
                    : filter(field.getValue(), List.of(), nextExcludes, true);
                if (value != null) {
                    filtered.set(field.getKey(), value);
                }
            } else if (nextIncludes.isEmpty() == false) {
                JsonNode child = filter(field.getValue(), nextIncludes, nextExcludes, false);
                if (child != null && isEmptyContainer(child) == false) {
                    filtered.set(field.getKey(), child);
                }
            }
        }
        return filtered;
    }

    private static List<Cursor> advanceAll(List<Cursor> cursors, String field) {
        List<Cursor> next = new ArrayList<>();
        for (Cursor cursor : cursors) {
            if (cursor.isSatisfied() == false) {
                next.addAll(cursor.advance(field));
            }
        }
        return next;
    }

    private static boolean anySatisfied(List<Cursor> cursors) {
        return cursors.stream().anyMatch(Cursor::isSatisfied);
    }

    private static boolean isEmptyContainer(JsonNode node) {
        return node.isContainerNode() && node.isEmpty();
    }
}
