/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.cluster.etcd;

import org.opensearch.OpenSearchStatusException;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.admin.cluster.health.ClusterHealthAction;
import org.opensearch.action.admin.cluster.health.ClusterHealthResponse;
import org.opensearch.action.support.ActionFilter;
import org.opensearch.action.support.ActionFilterChain;
import org.opensearch.action.support.clustermanager.ClusterManagerNodeReadRequest;
import org.opensearch.action.support.clustermanager.ClusterManagerNodeRequest;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.tasks.Task;

public class ClusterManagerActionFilter implements ActionFilter {
    private final ClusterService clusterService;

    public ClusterManagerActionFilter(ClusterService clusterService) {
        this.clusterService = clusterService;
    }

    @Override
    public int order() {
        return 0;
    }

    @Override
    public <Request extends ActionRequest, Response extends ActionResponse> void apply(
        Task task,
        String action,
        Request request,
        ActionListener<Response> listener,
        ActionFilterChain<Request, Response> chain
    ) {

        if (action.equals(ClusterHealthAction.NAME)) {
            ActionListener<ClusterHealthResponse> clusterHealthResponseListener = (ActionListener<ClusterHealthResponse>) listener;
            ClusterState clusterState = clusterService.state();
            String[] concreteIndices = clusterState.metadata().getConcreteAllIndices();
            ClusterHealthResponse clusterHealthResponse = new ClusterHealthResponse(
                clusterService.getClusterName().value(),
                concreteIndices,
                clusterState
            );
            clusterHealthResponseListener.onResponse(clusterHealthResponse);
            return;
        }
        if (request instanceof ClusterManagerNodeReadRequest<?> r) {
            r.local(true);
        } else if (request instanceof ClusterManagerNodeRequest<?> r) {
            listener.onFailure(
                new OpenSearchStatusException("Cannot execute action {} on clusterless node", RestStatus.BAD_REQUEST, action)
            );
            return;
        }
        chain.proceed(task, action, request, listener);
    }
}
