/*
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.router;

import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PathTrieTests {

    private static Route route(String method, String path, String group) {
        return new Route(method, path, group, false, Set.of());
    }

    @Test
    void literalSegmentBeatsWildcard() {
        PathTrie trie = new PathTrie();
        trie.insert(route("GET", "/_alias/{name}", "indices.get_alias"));
        trie.insert(route("GET", "/{index}/_alias", "indices.get_alias_for_index"));

        PathTrie.Match aliasByName = trie.resolve("GET", "/_alias/my-alias");
        assertNotNull(aliasByName);
        assertEquals("indices.get_alias", aliasByName.route().operationGroup());
        assertEquals("my-alias", aliasByName.pathParams().get("name"));

        PathTrie.Match aliasForIndex = trie.resolve("GET", "/my-index/_alias");
        assertNotNull(aliasForIndex);
        assertEquals("indices.get_alias_for_index", aliasForIndex.route().operationGroup());
        assertEquals("my-index", aliasForIndex.pathParams().get("index"));
    }

    /**
     * The case a non-backtracking router gets wrong: the literal {@code _cat}
     * child exists, so a greedy matcher commits to it and then fails at depth 2
     * instead of retreating to the {@code {index}} branch.
     */
    @Test
    void backtracksOutOfALiteralSubtreeThatDeadEnds() {
        PathTrie trie = new PathTrie();
        trie.insert(route("GET", "/_cat/indices", "cat.indices"));
        trie.insert(route("GET", "/{index}/_settings", "indices.get_settings"));

        // "_cat" matches a literal child at depth 1, but that node's only child is
        // the literal "indices" and it has no wildcard child, so "_settings" dead
        // ends. Resolution must retreat and try {index} instead.
        PathTrie.Match match = trie.resolve("GET", "/_cat/_settings");
        assertNotNull(match, "should backtrack out of the _cat subtree");
        assertEquals("indices.get_settings", match.route().operationGroup());
        assertEquals("_cat", match.pathParams().get("index"));

        // And the literal route still wins when it does match.
        assertEquals("cat.indices", trie.resolve("GET", "/_cat/indices").route().operationGroup());
    }

    @Test
    void deeperLiteralWinsOverWildcardAtSameDepth() {
        PathTrie trie = new PathTrie();
        trie.insert(route("PUT", "/{index}/_mapping", "indices.put_mapping"));
        trie.insert(route("PUT", "/_index_template/{name}", "indices.put_index_template"));

        assertEquals(
            "indices.put_index_template",
            trie.resolve("PUT", "/_index_template/logs").route().operationGroup()
        );
        assertEquals(
            "indices.put_mapping",
            trie.resolve("PUT", "/logs-000001/_mapping").route().operationGroup()
        );
    }

    @Test
    void distinguishesUnknownPathFromUnsupportedMethod() {
        PathTrie trie = new PathTrie();
        trie.insert(route("GET", "/{index}", "indices.get"));
        trie.insert(route("PUT", "/{index}", "indices.create"));

        assertNull(trie.resolve("POST", "/my-index"));
        assertNotNull(trie.resolvePath("/my-index"));
        assertEquals(Set.of("GET", "PUT"), trie.allowedMethods("/my-index"));

        assertNull(trie.resolvePath("/my-index/_nonexistent/deeper"));
        assertEquals(Set.of(), trie.allowedMethods("/my-index/_nonexistent/deeper"));
    }

    @Test
    void decodesCapturedSegmentsButNotBeforeSplitting() {
        PathTrie trie = new PathTrie();
        trie.insert(route("GET", "/{index}/_settings", "indices.get_settings"));

        // %2F must stay inside one segment rather than splitting the path.
        PathTrie.Match match = trie.resolve("GET", "/weird%2Fname/_settings");
        assertNotNull(match);
        assertEquals("weird/name", match.pathParams().get("index"));

        assertEquals("a b", PathTrie.decodeSegment("a%20b"));
        // '+' is a literal plus in a path segment, not a space.
        assertEquals("a+b", PathTrie.decodeSegment("a+b"));
    }

    @Test
    void emptySegmentIsNotCapturedAsAWildcard() {
        PathTrie trie = new PathTrie();
        trie.insert(route("GET", "/{index}/_settings", "indices.get_settings"));

        assertNull(trie.resolve("GET", "//_settings"));
    }

    @Test
    void rootPathResolves() {
        PathTrie trie = new PathTrie();
        trie.insert(route("GET", "/", "info"));

        assertEquals("info", trie.resolve("GET", "/").route().operationGroup());
    }

    @Test
    void trailingSlashIsEquivalent() {
        PathTrie trie = new PathTrie();
        trie.insert(route("GET", "/_cluster/health", "cluster.health"));

        assertNotNull(trie.resolve("GET", "/_cluster/health"));
        assertNotNull(trie.resolve("GET", "/_cluster/health/"));
    }

    @Test
    void rejectsDuplicateRoutes() {
        PathTrie trie = new PathTrie();
        trie.insert(route("GET", "/_cluster/health", "cluster.health"));

        IllegalArgumentException e = assertThrows(
            IllegalArgumentException.class,
            () -> trie.insert(route("GET", "/_cluster/health", "cluster.health_duplicate"))
        );
        assertTrue(e.getMessage().contains("duplicate route"));
    }

    /**
     * The spec names the same trie position differently across operations, so
     * placeholders bind from the matched route's own template rather than from
     * the shared node. See the {@link PathTrie} javadoc.
     */
    @Test
    void placeholderNamesComeFromTheMatchedRouteNotTheSharedNode() {
        PathTrie trie = new PathTrie();
        trie.insert(route("GET", "/{index}/_alias", "indices.get_alias"));
        trie.insert(route("POST", "/{alias}/_rollover", "indices.rollover"));

        PathTrie.Match alias = trie.resolve("GET", "/logs-000001/_alias");
        assertEquals(Set.of("index"), alias.pathParams().keySet());
        assertEquals("logs-000001", alias.pathParams().get("index"));

        PathTrie.Match rollover = trie.resolve("POST", "/logs/_rollover");
        assertEquals(Set.of("alias"), rollover.pathParams().keySet());
        assertEquals("logs", rollover.pathParams().get("alias"));
    }

    @Test
    void bindsMultiplePlaceholdersInOrder() {
        PathTrie trie = new PathTrie();
        trie.insert(route("PUT", "/{index}/_alias/{name}", "indices.put_alias"));

        PathTrie.Match match = trie.resolve("PUT", "/logs-000001/_alias/current");
        assertEquals("logs-000001", match.pathParams().get("index"));
        assertEquals("current", match.pathParams().get("name"));
    }
}
