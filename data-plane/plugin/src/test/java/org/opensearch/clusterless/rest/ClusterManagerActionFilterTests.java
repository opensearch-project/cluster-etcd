/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.clusterless.rest;

import org.opensearch.OpenSearchStatusException;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.admin.cluster.health.ClusterHealthAction;
import org.opensearch.action.admin.cluster.health.ClusterHealthRequest;
import org.opensearch.action.admin.indices.close.CloseIndexRequest;
import org.opensearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.opensearch.action.admin.indices.settings.get.GetSettingsRequest;
import org.opensearch.action.support.ActionFilterChain;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.tasks.Task;
import org.opensearch.test.OpenSearchTestCase;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * That an action needing a cluster manager fails immediately and says something useful.
 *
 * <p>The behaviour these protect was found by indexing a document into a real node. It waited thirty seconds and
 * answered {@code {"type":"cluster_manager_not_discovered_exception","reason":null,"status":503}} — a retryable
 * status for something that can never succeed, and a null reason where the real reason was "this write needs a
 * mapping change, and mappings come from the control plane".
 */
public class ClusterManagerActionFilterTests extends OpenSearchTestCase {

    /** No state needed for the refusal paths, which is most of them. */
    private static final java.util.function.Supplier<ClusterState> NO_STATE = () -> ClusterState.EMPTY_STATE;

    /** Records whether the chain was allowed to continue, which is the other half of every assertion here. */
    private static final class Chain implements ActionFilterChain<ActionRequest, ActionResponse> {

        private final AtomicBoolean proceeded = new AtomicBoolean();

        @Override
        public void proceed(Task task, String action, ActionRequest request, ActionListener<ActionResponse> listener) {
            proceeded.set(true);
        }
    }

    private static final class Refusal {
        private Exception failure;
        private ActionResponse response;
    }

    private Refusal apply(String action, ActionRequest request, Chain chain, java.util.function.Supplier<ClusterState> state) {
        return apply(action, request, chain, state, null);
    }

    private Refusal apply(
        String action,
        ActionRequest request,
        Chain chain,
        java.util.function.Supplier<ClusterState> state,
        String tenantHeader
    ) {
        Refusal outcome = new Refusal();
        new ClusterManagerActionFilter(state, "clusterless", () -> tenantHeader).apply(null, action, request, new ActionListener<ActionResponse>() {
            @Override
            public void onResponse(ActionResponse response) {
                outcome.response = response;
            }

            @Override
            public void onFailure(Exception e) {
                outcome.failure = e;
            }
        }, chain);
        return outcome;
    }

    public void testADynamicMappingUpdateIsRefusedImmediatelyAndNamesTheControlPlane() {
        Chain chain = new Chain();

        Refusal outcome = apply("indices:admin/mapping/auto_put", new PutMappingRequest("my-index"), chain, NO_STATE);

        assertFalse("it must not reach the action, which would wait for a manager", chain.proceeded.get());
        assertNotNull(outcome.failure);
        OpenSearchStatusException failure = (OpenSearchStatusException) outcome.failure;
        // 400 rather than 503: a client should stop, not back off and retry something that cannot work.
        assertEquals(RestStatus.BAD_REQUEST, failure.status());
        assertTrue(failure.getMessage(), failure.getMessage().contains("mappings are desired state"));
        assertTrue(failure.getMessage(), failure.getMessage().contains("control plane"));
    }

    public void testAnExplicitPutMappingIsRefusedWithSomewhereToSendIt() {
        Chain chain = new Chain();

        Refusal outcome = apply("indices:admin/mapping/put", new PutMappingRequest("my-index"), chain, NO_STATE);

        assertFalse(chain.proceeded.get());
        assertTrue(outcome.failure.getMessage(), outcome.failure.getMessage().contains("control plane"));
    }

    public void testAnyOtherMetadataMutationIsRefusedWithAReason() {
        Chain chain = new Chain();

        Refusal outcome = apply("indices:admin/close", new CloseIndexRequest("my-index"), chain, NO_STATE);

        assertFalse(chain.proceeded.get());
        assertTrue(outcome.failure.getMessage(), outcome.failure.getMessage().contains("changes cluster metadata"));
        assertTrue(outcome.failure.getMessage(), outcome.failure.getMessage().contains("indices:admin/close"));
    }

    public void testAReadRequestIsAnsweredLocallyRatherThanRefused() {
        // Reads are useful and harmless; routing them to a manager is the only problem, so they are marked local
        // and allowed through.
        Chain chain = new Chain();
        GetSettingsRequest request = new GetSettingsRequest();
        assertFalse("a read request is not local until this filter says so", request.local());

        Refusal outcome = apply("indices:monitor/settings/get", request, chain, NO_STATE);

        assertTrue("the read must proceed", chain.proceeded.get());
        assertTrue("and it must be answered from local state", request.local());
        assertNull(outcome.failure);
    }

    public void testInternalActionsAreLeftCompletelyAlone() {
        // The safety rule. Shard-started notifications and peer recovery are the server talking to itself, and a
        // filter that refused them would break the shard lifecycle this plugin exists to drive.
        Chain chain = new Chain();

        Refusal outcome = apply("internal:cluster/shard/started", new CloseIndexRequest("my-index"), chain, NO_STATE);

        assertTrue("internal traffic must pass through untouched", chain.proceeded.get());
        assertNull(outcome.failure);
    }

    public void testAnOrdinaryRequestIsNotIntercepted() {
        // Search, index, get: nothing to do with a cluster manager, and the filter is on the path for all of them.
        Chain chain = new Chain();

        Refusal outcome = apply("indices:data/read/search", new NotAClusterManagerRequest(), chain, NO_STATE);

        assertTrue(chain.proceeded.get());
        assertNull(outcome.failure);
    }

    public void testClusterHealthIsAnsweredFromLocalStateRatherThanHanging() {
        // The first thing anyone runs. Hanging on it makes a working node look dead.
        ClusterState state = ClusterState.builder(new ClusterName("clusterless")).build();
        Chain chain = new Chain();

        Refusal outcome = apply(ClusterHealthAction.NAME, new ClusterHealthRequest(), chain, () -> state);

        assertFalse(chain.proceeded.get());
        assertNull(outcome.failure);
        assertNotNull("health must be answered, not refused", outcome.response);
    }

    // ---------------------------------------------------------------- a tenant asking for a cluster-level view

    public void testATenantsClusterHealthIsRefusedAndNamesTheControlPlane() {
        // This node holds a part of the fleet, and its local health would be that part's: other tenants' indices
        // included, since it covers every index here. A tenant's view of the cluster is the control plane's to answer.
        ClusterState state = ClusterState.builder(new ClusterName("clusterless")).build();
        Chain chain = new Chain();

        Refusal outcome = apply(ClusterHealthAction.NAME, new ClusterHealthRequest(), chain, () -> state, "acme");

        assertFalse(chain.proceeded.get());
        assertNull(outcome.response);
        assertEquals(RestStatus.BAD_REQUEST, ((OpenSearchStatusException) outcome.failure).status());
        assertTrue(outcome.failure.getMessage(), outcome.failure.getMessage().contains("control plane"));
    }

    public void testATenantsClusterStateIsRefusedToo() {
        Chain chain = new Chain();

        Refusal outcome = apply(
            org.opensearch.action.admin.cluster.state.ClusterStateAction.NAME,
            new org.opensearch.action.admin.cluster.state.ClusterStateRequest(),
            chain,
            () -> ClusterState.EMPTY_STATE,
            "acme"
        );

        assertFalse("not answered from this node's state", chain.proceeded.get());
        assertEquals(RestStatus.BAD_REQUEST, ((OpenSearchStatusException) outcome.failure).status());
    }

    public void testWithoutATenantTheNodeAnswersWithItsOwnView() {
        // For the platform team: what this node believes, which is what debugging needs.
        ClusterState state = ClusterState.builder(new ClusterName("clusterless")).build();
        Chain healthChain = new Chain();
        assertNotNull(apply(ClusterHealthAction.NAME, new ClusterHealthRequest(), healthChain, () -> state, " ").response);

        Chain stateChain = new Chain();
        org.opensearch.action.admin.cluster.state.ClusterStateRequest stateRequest = new org.opensearch.action.admin.cluster.state.ClusterStateRequest();
        apply(org.opensearch.action.admin.cluster.state.ClusterStateAction.NAME, stateRequest, stateChain, () -> state, null);
        assertTrue(stateChain.proceeded.get());
        assertTrue("and locally", stateRequest.local());
    }

    /** A request that has nothing to do with a cluster manager. */
    private static final class NotAClusterManagerRequest extends ActionRequest {
        @Override
        public ActionRequestValidationException validate() {
            return null;
        }
    }

}
