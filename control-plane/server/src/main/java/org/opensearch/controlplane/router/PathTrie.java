/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.router;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * A path trie with OpenSearch's resolution semantics.
 *
 * <p>This deliberately does not delegate to a web framework's router. OpenSearch's
 * {@code org.opensearch.common.path.PathTrie} resolves a concrete path against
 * registered templates by preferring a literal segment match at each level and
 * <em>backtracking</em> to the wildcard child when the literal subtree yields no
 * handler. Framework routers generally use a different, non-backtracking
 * precedence, which produces divergent behaviour on genuinely ambiguous
 * OpenSearch paths such as {@code /_alias/{name}} versus {@code /{index}/_alias}.
 * Matching the server here is the whole point.
 *
 * <h2>Placeholder naming</h2>
 *
 * <p>Placeholder names are stored per-route, not per-trie-node, and bound by
 * zipping the matched wildcard segments against the matched route's own template.
 * This matters because the spec names the same trie position differently across
 * operations: {@code indices.get_alias} is {@code GET /{index}/_alias} while
 * {@code indices.rollover} is {@code POST /{alias}/_rollover}, so the root
 * wildcard is {@code index} for one and {@code alias} for the other.
 *
 * <p>OpenSearch's own trie stores the name on the shared node, so whichever
 * template was inserted last wins and every operation at that position sees the
 * same name — which is why {@code RestRolloverIndexAction} reads
 * {@code request.param("index")} despite the documented path saying
 * {@code {alias}}. Binding per-route instead gives each handler the names its own
 * spec template declares. That is a divergence from the server, but only in
 * internal naming: the paths route identically, and placeholder names never
 * appear on the wire.
 *
 * <p>Not thread-safe for writes. Build it fully, then read concurrently.
 */
public final class PathTrie {

    /** The result of resolving a concrete request path and method. */
    public record Match(Route route, Map<String, String> pathParams) {}

    /**
     * A resolved path node: which methods it serves, so the caller can tell
     * "wrong method on a known path" (405) from "no such path" (400).
     *
     * @param wildcardValues matched wildcard segments in order, still encoded
     */
    public record PathMatch(Map<String, Route> routesByMethod, List<String> wildcardValues) {

        /** Binds this match's wildcard values to the placeholder names of one route's template. */
        public Map<String, String> pathParams(Route route) {
            List<String> names = placeholderNames(route.path());
            if (names.size() != wildcardValues.size()) {
                throw new IllegalStateException(
                    "route [" + route + "] declares " + names.size() + " placeholders but "
                        + wildcardValues.size() + " wildcard segments matched"
                );
            }
            Map<String, String> params = new LinkedHashMap<>();
            for (int i = 0; i < names.size(); i++) {
                params.put(names.get(i), decodeSegment(wildcardValues.get(i)));
            }
            return Collections.unmodifiableMap(params);
        }
    }

    private static final class Node {
        /** Literal children, keyed by exact segment text. */
        private final Map<String, Node> children = new HashMap<>();
        /** The single wildcard child, if any. Its name is not stored here; see the class javadoc. */
        private Node wildcardChild;
        /** Routes terminating at this node, keyed by uppercase HTTP method. */
        private final Map<String, Route> routes = new LinkedHashMap<>();

        boolean isLeafWithRoutes() {
            return routes.isEmpty() == false;
        }
    }

    private final Node root = new Node();
    private final List<Route> routes = new ArrayList<>();

    /** Inserts a route. Throws if the same (method, path) is registered twice. */
    public void insert(Route route) {
        String[] segments = splitTemplate(route.path());
        Node current = root;

        for (String segment : segments) {
            if (isPlaceholder(segment)) {
                if (current.wildcardChild == null) {
                    current.wildcardChild = new Node();
                }
                current = current.wildcardChild;
            } else {
                current = current.children.computeIfAbsent(segment, key -> new Node());
            }
        }

        Route existing = current.routes.putIfAbsent(route.method(), route);
        if (existing != null) {
            throw new IllegalArgumentException(
                "duplicate route " + route.method() + " " + route.path()
                    + " (operation groups [" + existing.operationGroup() + "] and [" + route.operationGroup() + "])"
            );
        }
        routes.add(route);
    }

    /**
     * Resolves a concrete request path, preferring literal segments and
     * backtracking into the wildcard child when the literal subtree has nothing
     * to offer.
     *
     * @param requestPath a concrete path with the query string stripped, still
     *                    percent-encoded, e.g. {@code /my-index/_alias/a}
     * @return the match, or {@code null} if no registered template serves this path
     */
    public PathMatch resolvePath(String requestPath) {
        String[] segments = splitTemplate(requestPath);
        List<String> wildcardValues = new ArrayList<>(4);
        Node found = descend(root, segments, 0, wildcardValues);
        if (found == null) {
            return null;
        }
        return new PathMatch(Collections.unmodifiableMap(found.routes), List.copyOf(wildcardValues));
    }

    /** Resolves both path and method, binding path parameters from the matched route's template. */
    public Match resolve(String method, String requestPath) {
        PathMatch pathMatch = resolvePath(requestPath);
        if (pathMatch == null) {
            return null;
        }
        Route route = pathMatch.routesByMethod().get(method.toUpperCase(Locale.ROOT));
        return route == null ? null : new Match(route, pathMatch.pathParams(route));
    }

    private Node descend(Node node, String[] segments, int index, List<String> wildcardValues) {
        if (index == segments.length) {
            return node.isLeafWithRoutes() ? node : null;
        }
        String segment = segments[index];

        Node literal = node.children.get(segment);
        if (literal != null) {
            Node result = descend(literal, segments, index + 1, wildcardValues);
            if (result != null) {
                return result;
            }
        }

        // An empty segment must not be captured as a placeholder value:
        // OpenSearch treats //_alias as a malformed path rather than binding
        // index="".
        if (node.wildcardChild != null && segment.isEmpty() == false) {
            int mark = wildcardValues.size();
            wildcardValues.add(segment);
            Node result = descend(node.wildcardChild, segments, index + 1, wildcardValues);
            if (result != null) {
                return result;
            }
            // This branch dead-ended; discard anything it speculatively bound.
            while (wildcardValues.size() > mark) {
                wildcardValues.removeLast();
            }
        }

        return null;
    }

    /** All methods registered for the given path, for the {@code Allow} header. */
    public Set<String> allowedMethods(String requestPath) {
        PathMatch match = resolvePath(requestPath);
        return match == null ? Set.of() : new TreeSet<>(match.routesByMethod().keySet());
    }

    public List<Route> routes() {
        return Collections.unmodifiableList(routes);
    }

    public int size() {
        return routes.size();
    }

    /** The placeholder names in a path template, in order. */
    static List<String> placeholderNames(String pathTemplate) {
        List<String> names = new ArrayList<>(4);
        for (String segment : splitTemplate(pathTemplate)) {
            if (isPlaceholder(segment)) {
                names.add(segment.substring(1, segment.length() - 1));
            }
        }
        return names;
    }

    private static boolean isPlaceholder(String segment) {
        return segment.length() > 2 && segment.charAt(0) == '{' && segment.charAt(segment.length() - 1) == '}';
    }

    /**
     * Percent-decodes a single captured path segment.
     *
     * <p>Decoding happens per-segment, after splitting, never before: an index
     * name may legitimately contain {@code %2F}, and decoding the whole path
     * first would split it into two segments and route the request somewhere
     * else entirely.
     *
     * <p>This deliberately does <em>not</em> map {@code +} to a space. In a path
     * segment {@code +} is a literal plus per RFC 3986. OpenSearch routes paths
     * through {@code RestUtils.decodeComponent}, which may treat {@code +} as a
     * space here; that is worth confirming against a live cluster, since an index
     * named {@code a+b} would then diverge.
     */
    static String decodeSegment(String segment) {
        if (segment.indexOf('%') < 0) {
            return segment;
        }
        byte[] bytes = segment.getBytes(StandardCharsets.UTF_8);
        ByteArrayOutputStream out = new ByteArrayOutputStream(bytes.length);
        for (int i = 0; i < bytes.length; i++) {
            if (bytes[i] == '%' && i + 2 < bytes.length) {
                int high = Character.digit(bytes[i + 1], 16);
                int low = Character.digit(bytes[i + 2], 16);
                if (high >= 0 && low >= 0) {
                    out.write((high << 4) + low);
                    i += 2;
                    continue;
                }
            }
            out.write(bytes[i]);
        }
        return out.toString(StandardCharsets.UTF_8);
    }

    /**
     * Splits on {@code /}, dropping the leading empty segment and any single
     * trailing empty segment. A few spec paths are written with a trailing slash
     * (for example some {@code _plugins/_security} routes), and OpenSearch serves
     * those identically to the un-slashed form.
     *
     * <p>Segments are returned undecoded; see {@link #decodeSegment}.
     */
    static String[] splitTemplate(String path) {
        String trimmed = path;
        if (trimmed.startsWith("/")) {
            trimmed = trimmed.substring(1);
        }
        if (trimmed.endsWith("/")) {
            trimmed = trimmed.substring(0, trimmed.length() - 1);
        }
        if (trimmed.isEmpty()) {
            return new String[0];
        }
        return trimmed.split("/", -1);
    }
}
