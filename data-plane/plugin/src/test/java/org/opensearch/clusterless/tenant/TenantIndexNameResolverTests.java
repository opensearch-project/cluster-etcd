/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.clusterless.tenant;

import org.opensearch.Version;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.clusterless.ClusterlessIndexSettings;
import org.opensearch.clusterless.contract.TenantId;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.Optional;

/**
 * Mapping a tenant's index names, and the two properties that make doing so safe.
 *
 * <p>Most of these are about isolation rather than convenience. A resolver that rewrites index names can send a
 * request anywhere, so the tests worth having are the ones that pin what it will <em>not</em> do: it will not map
 * across tenants, and with no tenant it will not map at all.
 */
public class TenantIndexNameResolverTests extends OpenSearchTestCase {

    private static final TenantId ACME = TenantId.of("acme");
    private static final TenantId GLOBEX = TenantId.of("globex");

    // ---------------------------------------------------------------- the mapping

    public void testATenantsOwnNameResolvesToTheFabricatedOne() {
        List<String> resolved = resolveFor(ACME, List.of("my-index"));

        assertEquals(List.of("acme.my-index.aaaaaaaaaa"), resolved);
    }

    public void testAnIndexOfAnotherTenantWithTheSameNameIsNotReached() {
        // The property that makes the whole idea safe: the tenant is part of every match, so an ambiguous name
        // cannot cross tenants however the fabricated names happen to sort.
        List<String> resolved = resolveFor(GLOBEX, List.of("my-index"));

        assertEquals(List.of("globex.my-index.bbbbbbbbbb"), resolved);
    }

    public void testAllResolvesToOnlyTheAskingTenantsIndices() {
        // The more important half. GET /_search with no index becomes _all, which without this resolves to every
        // index in the state -- a cross-tenant read on a node serving several tenants.
        assertEquals(
            List.of("acme.my-index.aaaaaaaaaa", "acme.other.cccccccccc"),
            resolveFor(ACME, List.of(Metadata.ALL))
        );
        assertEquals(List.of("globex.my-index.bbbbbbbbbb"), resolveFor(GLOBEX, List.of("*")));
    }

    public void testAWildcardMatchesTheTenantsNamesRatherThanTheFabricatedOnes() {
        // A tenant thinks in its own names, so `my-*` has to be matched against those. Matching the fabricated
        // names would also make `acme.*` a way for one tenant to enumerate another.
        assertEquals(List.of("acme.my-index.aaaaaaaaaa"), resolveFor(ACME, List.of("my-*")));
        assertEquals(List.of(), concreteFor(GLOBEX, SEARCH, "acme.*"));
    }

    // ---------------------------------------------------------------- what it refuses to do

    public void testWithNoTenantNothingIsRewritten() {
        // Not a default and not an error. An unqualified name then fails to resolve, which is the safe outcome,
        // and a fabricated name typed in full still works -- so an operator on a node with no auth layer is not
        // locked out.
        List<String> resolved = resolve(TenantResolver.none(), List.of("my-index", Metadata.ALL));

        assertEquals(List.of("my-index", Metadata.ALL), resolved);
    }

    public void testANameThisNodeDoesNotKnowIsNotFoundUnderTheNameAskedFor() {
        IndexNotFoundException e = expectThrows(IndexNotFoundException.class, () -> concreteFor(ACME, SEARCH, "nonesuch"));
        assertEquals("nonesuch", e.getIndex().getName());
        assertEquals(List.of(), concreteFor(ACME, IndicesOptions.lenientExpandOpen(), "nonesuch"));
    }

    public void testAnAmbiguousNameResolvesToNeitherIndex() {
        // One tenant, one name, two indices in the state: the name was deleted and recreated while this node still
        // retains the old index's metadata so that its files survive. Resolving it would pick one by scan order,
        // and picking the old one answers from an index the tenant believes is gone.
        ClusterState state = state(
            index("acme.my-index.aaaaaaaaaa", ACME, "my-index"),
            index("acme.my-index.dddddddddd", ACME, "my-index")
        );

        IndicesOptions search = SearchRequest.DEFAULT_INDICES_OPTIONS;
        IndexNameExpressionResolver chain = chain(fixed(ACME));
        expectThrows(IndexNotFoundException.class, () -> chain.concreteIndexNames(state, search, "my-index"));

        // Which is why the warning says to use the full name, and that still works.
        String[] full = chain.concreteIndexNames(state, search, "acme.my-index.dddddddddd");
        assertArrayEquals(new String[] { "acme.my-index.dddddddddd" }, full);
    }

    // ---------------------------------------------------------------- through the server's own resolver chain

    // The date-math and wildcard resolvers run after this one, and the wildcard resolver reads an empty list as
    // _all. So what this resolver returns is not the answer; what the chain returns is, and these assert that.

    public void testATenantWithNoIndicesHereFindsNothingRatherThanEverything() {
        TenantId initech = TenantId.of("initech");

        assertEquals(List.of(), concreteFor(initech, SEARCH));
        assertEquals(List.of(), concreteFor(initech, SEARCH, Metadata.ALL));
        assertEquals(List.of(), concreteFor(initech, SEARCH, "my-*"));
        expectThrows(IndexNotFoundException.class, () -> concreteFor(initech, STRICT, "my-*"));
    }

    public void testAWildcardMatchingNoneOfTheTenantsIndicesFindsNothing() {
        assertEquals(List.of(), concreteFor(GLOBEX, SEARCH, "other*"));
        expectThrows(IndexNotFoundException.class, () -> concreteFor(GLOBEX, STRICT, "other*"));
    }

    public void testAnotherTenantsFabricatedNameIsNotFound() {
        // A fabricated name is not a secret -- it is the _index of every hit -- so knowing one must not reach it.
        IndexNotFoundException e = expectThrows(
            IndexNotFoundException.class,
            () -> concreteFor(GLOBEX, SEARCH, "acme.my-index.aaaaaaaaaa")
        );
        assertEquals("acme.my-index.aaaaaaaaaa", e.getIndex().getName());
        assertEquals(List.of(), concreteFor(GLOBEX, IndicesOptions.lenientExpandOpen(), "acme.my-index.aaaaaaaaaa"));
        assertEquals(List.of("acme.my-index.aaaaaaaaaa"), concreteFor(ACME, SEARCH, "acme.my-index.aaaaaaaaaa"));
    }

    public void testAnExclusionRemovesTheTenantsIndex() {
        assertEquals(List.of("acme.my-index.aaaaaaaaaa"), concreteFor(ACME, SEARCH, "*", "-other"));
        assertEquals(List.of(), concreteFor(GLOBEX, SEARCH, "*", "-my-index"));
    }

    public void testResolvingExpressionsWithNoneGivenStaysWithinTheTenant() {
        // resolveExpressions does not turn an empty request into _all before the chain, as concreteIndices does.
        IndexNameExpressionResolver chain = chain(fixed(TenantId.of("initech")));
        assertEquals(java.util.Set.of(), chain.resolveExpressions(defaultState()));
        assertEquals(java.util.Set.of(), chain.resolveExpressions(defaultState(), "my-*"));
    }

    // ---------------------------------------------------------------- resolving the tenant itself

    public void testADeclaredTenantRefusesARequestAskingForAnother() {
        // Serving this node's tenant instead would answer a question nobody asked, with data the caller may not be
        // entitled to. Empty fails the request closed.
        org.opensearch.common.util.concurrent.ThreadContext context = threadContext();
        context.putHeader(TenantResolver.HEADER, "globex");

        assertEquals(Optional.empty(), TenantResolver.fixed(ACME, context).current());
    }

    public void testADeclaredTenantAcceptsARequestThatAgreesOrSaysNothing() {
        assertEquals(Optional.of(ACME), TenantResolver.fixed(ACME, threadContext()).current());

        org.opensearch.common.util.concurrent.ThreadContext agreeing = threadContext();
        agreeing.putHeader(TenantResolver.HEADER, "acme");
        assertEquals(Optional.of(ACME), TenantResolver.fixed(ACME, agreeing).current());
    }

    public void testTheHeaderIsReadAndAMalformedOneIsRefusedRatherThanSanitised() {
        org.opensearch.common.util.concurrent.ThreadContext context = threadContext();
        context.putHeader(TenantResolver.HEADER, "acme");
        assertEquals(Optional.of(ACME), TenantResolver.fromHeader(context).current());

        // A tenant id becomes a key prefix, so a value containing a slash is indistinguishable from an attempt to
        // escape the namespace. The control plane rejects rather than sanitises, and so does this.
        org.opensearch.common.util.concurrent.ThreadContext escaping = threadContext();
        escaping.putHeader(TenantResolver.HEADER, "acme/../globex");
        assertEquals(Optional.empty(), TenantResolver.fromHeader(escaping).current());
    }

    public void testNoHeaderIsNoTenant() {
        assertEquals(Optional.empty(), TenantResolver.fromHeader(threadContext()).current());
    }

    // ---------------------------------------------------------------- fixtures

    private static org.opensearch.common.util.concurrent.ThreadContext threadContext() {
        return new org.opensearch.common.util.concurrent.ThreadContext(Settings.EMPTY);
    }

    private static TenantResolver fixed(TenantId tenant) {
        return () -> Optional.of(tenant);
    }

    /** What {@code GET /<expressions>/_search} uses. Lenient about a wildcard matching nothing, not about a name. */
    private static final IndicesOptions SEARCH = SearchRequest.DEFAULT_INDICES_OPTIONS;

    /** Strict about both: a wildcard matching nothing is an error too. */
    private static final IndicesOptions STRICT = IndicesOptions.fromOptions(false, false, true, false);

    private static IndexNameExpressionResolver chain(TenantResolver tenants) {
        return new IndexNameExpressionResolver(threadContext(), List.of(new TenantIndexNameResolver(tenants)));
    }

    private static List<String> concreteFor(TenantId tenant, IndicesOptions options, String... expressions) {
        String[] names = chain(fixed(tenant)).concreteIndexNames(defaultState(), options, expressions);
        return java.util.Arrays.stream(names).sorted().toList();
    }

    private static List<String> resolveFor(TenantId tenant, List<String> expressions) {
        return resolve(fixed(tenant), expressions);
    }

    private static List<String> resolve(TenantResolver tenants, List<String> expressions) {
        return resolve(tenants, expressions, defaultState());
    }

    private static List<String> resolve(TenantResolver tenants, List<String> expressions, ClusterState state) {
        IndexNameExpressionResolver.Context context = new IndexNameExpressionResolver.Context(
            state,
            IndicesOptions.lenientExpandOpen(),
            false
        );
        return new TenantIndexNameResolver(tenants).resolve(context, expressions);
    }

    /** Two tenants, one shared index name, and one index that only the first tenant has. */
    private static ClusterState defaultState() {
        return state(
            index("acme.my-index.aaaaaaaaaa", ACME, "my-index"),
            index("globex.my-index.bbbbbbbbbb", GLOBEX, "my-index"),
            index("acme.other.cccccccccc", ACME, "other")
        );
    }

    private static ClusterState state(IndexMetadata... indices) {
        Metadata.Builder metadata = Metadata.builder();
        for (IndexMetadata index : indices) {
            metadata.put(index, false);
        }
        return ClusterState.builder(new ClusterName("clusterless")).metadata(metadata).build();
    }

    private static IndexMetadata index(String fabricatedName, TenantId tenant, String friendlyName) {
        return IndexMetadata.builder(fabricatedName)
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(IndexMetadata.SETTING_INDEX_UUID, fabricatedName + "-uuid")
                    .put(ClusterlessIndexSettings.TENANT.getKey(), tenant.value())
                    .put(ClusterlessIndexSettings.TENANT_INDEX_NAME.getKey(), friendlyName)
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            )
            .build();
    }
}
