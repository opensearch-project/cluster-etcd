/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.resolver;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Every rule here was checked against OpenSearch 3.9.0. The ones flagged as such are the rules a
 * plausible implementation gets wrong.
 */
class IndexNameExpressionResolverTests {

    /** cp-a, cp-b, other, .cp-hidden (hidden), closed-one (closed), cp-alias -> cp-a + cp-b. */
    private static IndexNameExpressionResolver resolver() {
        Map<String, IndexAbstraction> lookup = new HashMap<>();
        lookup.put("cp-a", IndexAbstraction.index("cp-a", false, true));
        lookup.put("cp-b", IndexAbstraction.index("cp-b", false, true));
        lookup.put("other", IndexAbstraction.index("other", false, true));
        lookup.put(".cp-hidden", IndexAbstraction.index(".cp-hidden", true, true));
        lookup.put("closed-one", IndexAbstraction.index("closed-one", false, false));
        lookup.put("cp-alias", IndexAbstraction.alias("cp-alias", List.of("cp-a", "cp-b"), false));
        return new IndexNameExpressionResolver(new IndexAbstractions(lookup));
    }

    private static List<String> resolve(String expression, IndicesOptions options) {
        IndexResolution resolution = resolver().resolve(expression, options);
        return assertInstanceOf(IndexResolution.Resolved.class, resolution).indices().stream().sorted().toList();
    }

    private static List<String> resolve(String expression) {
        return resolve(expression, IndicesOptions.strictExpandOpen());
    }

    // ------------------------------------------------------------------
    // the basics
    // ------------------------------------------------------------------

    @Test
    void resolvesAConcreteName() {
        assertEquals(List.of("cp-a"), resolve("cp-a"));
    }

    @Test
    void resolvesACommaSeparatedList() {
        assertEquals(List.of("cp-a", "other"), resolve("cp-a,other"));
    }

    @Test
    void expandsAWildcard() {
        assertEquals(List.of("cp-a", "cp-b"), resolve("cp-*"));
    }

    @Test
    void flattensAnAliasToItsIndices() {
        assertEquals(List.of("cp-a", "cp-b"), resolve("cp-alias"));
    }

    @Test
    void trivialExpressionsMeanEverything() {
        // Hidden and closed are both left out under strictExpandOpen.
        List<String> all = List.of("cp-a", "cp-b", "other");
        assertEquals(all, resolve(null));
        assertEquals(all, resolve("_all"));
        assertEquals(all, resolve("*"));
    }

    // ------------------------------------------------------------------
    // exclusions -- the rule most likely to be implemented wrongly
    // ------------------------------------------------------------------

    /** Verified: {@code cp-*,-cp-a} excludes, but a leading {@code -} alone does not. */
    @Test
    void exclusionAppliesOnlyAfterAWildcard() {
        assertEquals(List.of("cp-b"), resolve("cp-*,-cp-a"));

        // With no preceding wildcard, -cp-a is a name, and no index is called that.
        IndexResolution resolution = resolver().resolve("-cp-a", IndicesOptions.strictExpandOpen());
        IndexResolution.NotFound notFound = assertInstanceOf(IndexResolution.NotFound.class, resolution);
        assertEquals("-cp-a", notFound.expression());
    }

    @Test
    void exclusionCanRemoveAWholePattern() {
        assertEquals(List.of("other"), resolve("*,-cp-*"));
    }

    @Test
    void exclusionCanRemoveAnAliasesMembers() {
        assertEquals(List.of("other"), resolve("*,-cp-alias"));
    }

    @Test
    void orderMattersBecauseExclusionsApplyAsTheyAreSeen() {
        // Removed, then added back.
        assertEquals(List.of("cp-a", "cp-b", "other"), resolve("*,-cp-*,cp-*"));
    }

    // ------------------------------------------------------------------
    // hidden indices
    // ------------------------------------------------------------------

    /** Verified: {@code *} skips a hidden index but {@code .cp-*} finds it. */
    @Test
    void aDotPrefixedPatternReachesHiddenIndicesWithoutTheHiddenOption() {
        assertTrue(resolve("*").contains(".cp-hidden") == false);
        assertEquals(List.of(".cp-hidden"), resolve(".cp-*"));
    }

    @Test
    void hiddenExpansionIncludesThemForAnyPattern() {
        IndicesOptions withHidden = new IndicesOptions(true, false, true, false, true);
        assertTrue(resolve("*", withHidden).contains(".cp-hidden"));
    }

    @Test
    void aHiddenIndexIsStillReachableByExactName() {
        assertEquals(List.of(".cp-hidden"), resolve(".cp-hidden"));
    }

    // ------------------------------------------------------------------
    // open / closed
    // ------------------------------------------------------------------

    @Test
    void closedIndicesAreExcludedFromExpansionByDefault() {
        assertTrue(resolve("*").contains("closed-one") == false);
    }

    @Test
    void expandingClosedIncludesThem() {
        IndicesOptions closedToo = new IndicesOptions(true, true, false, false, true);
        assertTrue(resolve("*", closedToo).contains("closed-one"));
    }

    @Test
    void expandingOnlyClosedExcludesOpen() {
        IndicesOptions onlyClosed = new IndicesOptions(false, true, false, false, true);
        assertEquals(List.of("closed-one"), resolve("*", onlyClosed));
    }

    // ------------------------------------------------------------------
    // errors
    // ------------------------------------------------------------------

    /** Verified: a leading underscore is invalid_index_name_exception, not index_not_found. */
    @Test
    void aLeadingUnderscoreIsAnInvalidNameNotAMissingOne() {
        IndexResolution resolution = resolver().resolve("_foo", IndicesOptions.strictExpandOpen());

        IndexResolution.InvalidName invalid = assertInstanceOf(IndexResolution.InvalidName.class, resolution);
        assertEquals("_foo", invalid.expression());
        assertEquals("must not start with '_'.", invalid.reason());
    }

    @Test
    void aMissingConcreteNameIsNotFoundUnlessIgnored() {
        assertInstanceOf(
            IndexResolution.NotFound.class,
            resolver().resolve("cp-a,missing", IndicesOptions.strictExpandOpen())
        );
        // ignore_unavailable skips it instead.
        assertEquals(List.of("cp-a"), resolve("cp-a,missing", IndicesOptions.lenient()));
    }

    @Test
    void aPatternMatchingNothingDependsOnAllowNoIndices() {
        assertEquals(List.of(), resolve("nomatch-*"));

        IndicesOptions strict = new IndicesOptions(true, false, false, false, false);
        assertInstanceOf(IndexResolution.NotFound.class, resolver().resolve("nomatch-*", strict));
    }

    // ------------------------------------------------------------------
    // details worth pinning
    // ------------------------------------------------------------------

    /** An exact match is never reinterpreted as a pattern, so a literal star in a name is reachable. */
    @Test
    void anExactMatchWinsOverPatternInterpretation() {
        Map<String, IndexAbstraction> lookup = new HashMap<>();
        lookup.put("a*b", IndexAbstraction.index("a*b", false, true));
        lookup.put("axb", IndexAbstraction.index("axb", false, true));
        var resolver = new IndexNameExpressionResolver(new IndexAbstractions(lookup));

        IndexResolution resolution = resolver.resolve("a*b", IndicesOptions.strictExpandOpen());
        assertEquals(List.of("a*b"), assertInstanceOf(IndexResolution.Resolved.class, resolution).indices());
    }

    /** {@code ?} is not a wildcard: OpenSearch's isSimpleMatchPattern tests for {@code *} only. */
    @Test
    void questionMarkIsALiteralCharacter() {
        assertInstanceOf(
            IndexResolution.NotFound.class,
            resolver().resolve("cp-?", IndicesOptions.strictExpandOpen())
        );
    }

    @Test
    void expandWildcardsNoneLeavesLiteralsAndDropsPatterns() {
        IndicesOptions none = new IndicesOptions(false, false, false, false, true);

        assertEquals(List.of("cp-a"), resolve("cp-a", none));
        assertEquals(List.of(), resolve("cp-*", none));
    }

    /**
     * {@code expand_wildcards} governs what a pattern expands to, not what an explicit name means, so a
     * closed index named outright still resolves. In OpenSearch the state check sits inside
     * {@code expand()} rather than filtering the resolver's output.
     */
    @Test
    void expandWildcardsDoesNotFilterExplicitlyNamedIndices() {
        assertEquals(List.of("closed-one"), resolve("closed-one"), "named directly under expand=open");
        assertTrue(resolve("*").contains("closed-one") == false, "but a pattern still skips it");
    }

    @Test
    void resultsAreDeduplicated() {
        // cp-alias and cp-a both name cp-a.
        assertEquals(List.of("cp-a", "cp-b"), resolve("cp-alias,cp-a"));
    }

    @Test
    void anEmptyLookupResolvesEverythingToNothing() {
        var empty = new IndexNameExpressionResolver(new IndexAbstractions(Map.of()));

        assertEquals(
            List.of(),
            assertInstanceOf(IndexResolution.Resolved.class, empty.resolve("*", IndicesOptions.strictExpandOpen()))
                .indices()
        );
    }
}
