/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.etcd;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.admin.cluster.health.ClusterHealthAction;
import org.opensearch.action.admin.cluster.health.ClusterHealthResponse;
import org.opensearch.action.support.ActionFilter;
import org.opensearch.action.support.ActionFilterChain;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.tasks.Task;

public class ClusterHealthActionFilter implements ActionFilter {
    private final ClusterService clusterService;

    public ClusterHealthActionFilter(ClusterService clusterService) {
        this.clusterService = clusterService;
    }

    @Override
    public int order() {
        return 0;
    }

    @Override
    public <Request extends ActionRequest, Response extends ActionResponse> void apply(Task task, String action, Request request, ActionListener<Response> listener, ActionFilterChain<Request, Response> chain) {

        if (action.equals(ClusterHealthAction.NAME)) {
            ActionListener<ClusterHealthResponse> clusterHealthResponseListener = (ActionListener<ClusterHealthResponse>) listener;
            ClusterState clusterState = clusterService.state();
            String[] concreteIndices = clusterState.metadata().getConcreteAllIndices();
            ClusterHealthResponse clusterHealthResponse = new ClusterHealthResponse(clusterService.getClusterName().value(), concreteIndices, clusterState);
            clusterHealthResponseListener.onResponse(clusterHealthResponse);
            return;
        }
        chain.proceed(task, action, request, listener);
    }
}
