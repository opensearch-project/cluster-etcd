/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.clusterless;

import org.opensearch.action.support.IndicesOptions;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Collection;
import java.util.List;

/**
 * What the plugin is allowed to assume when each of its callbacks runs.
 *
 * <p>One test, because one callback got it wrong and the cost was total: the node did not start.
 */
public class ClusterlessPluginTests extends OpenSearchTestCase {

    public void testTheIndexNameResolverSurvivesBeingAskedForBeforeAnythingIsWired() {
        // ClusterModule is constructed at Node.<init>:830 and asks for the custom resolvers there. createComponents
        // -- the first callback handed a ThreadPool, an Environment or a ClusterService -- runs at line 1212. So a
        // resolver that captured any of those at construction threw NullPointerException out of a plugin callback
        // during bootstrap, and the whole node failed with it.
        ClusterlessPlugin plugin = new ClusterlessPlugin();

        Collection<IndexNameExpressionResolver.ExpressionResolver> resolvers = plugin.getIndexNameCustomResolvers();

        assertEquals(1, resolvers.size());
        IndexNameExpressionResolver.Context context = new IndexNameExpressionResolver.Context(
            ClusterState.builder(new ClusterName("clusterless")).build(),
            IndicesOptions.lenientExpandOpen(),
            false
        );
        // And it has to answer, not merely exist. With no tenant resolvable it rewrites nothing, which is the same
        // answer it gives a request that carries no tenant -- an unqualified name then fails to resolve rather than
        // resolving to another tenant's data.
        List<String> expressions = List.of("my-index", Metadata.ALL);
        assertEquals(expressions, resolvers.iterator().next().resolve(context, expressions));
    }
}
