/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.clusterless.rest;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.admin.cluster.health.ClusterHealthAction;
import org.opensearch.action.admin.cluster.health.ClusterHealthResponse;
import org.opensearch.action.admin.cluster.state.ClusterStateAction;
import org.opensearch.action.support.ActionFilter;
import org.opensearch.action.support.ActionFilterChain;
import org.opensearch.action.support.clustermanager.ClusterManagerNodeReadRequest;
import org.opensearch.action.support.clustermanager.ClusterManagerNodeRequest;
import org.opensearch.cluster.ClusterState;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.tasks.Task;

/**
 * Answers or refuses the actions that would otherwise wait for a cluster manager.
 *
 * <p>D5.2, and the requirement is sharper than it reads: such an action must be <em>"answered locally or refused
 * with a clear reason, never left to hang waiting for a manager that does not exist"</em>. Without this the
 * hanging is real and unhelpfully disguised. Indexing a document into an index whose mapping does not cover it
 * makes the server send an {@code indices:admin/mapping/auto_put} to the cluster manager, wait
 * {@code indices.mapping.dynamic_timeout} — thirty seconds by default — and then answer:
 *
 * <pre>
 *   {"type":"cluster_manager_not_discovered_exception","reason":null,"status":503}
 * </pre>
 *
 * <p>A 503 with a null reason after thirty seconds tells a client to retry something that can never succeed, and
 * tells a person nothing at all. What is actually happening is that the write needs a mapping change, and in this
 * architecture mappings come from the control plane — so the honest answer is an immediate refusal that says so.
 *
 * <h2>What it does with each kind of request</h2>
 *
 * <ul>
 *   <li><b>Read requests</b> ({@link ClusterManagerNodeReadRequest}) are marked local and allowed through, so
 *       they are answered from this node's own state instead of being routed to a manager.
 *   <li><b>Everything else that needs a manager</b> ({@link ClusterManagerNodeRequest}) is refused with
 *       {@code 400} and a reason naming the control plane. {@code 400} rather than {@code 503} deliberately: the
 *       request is not going to work later, so a client should stop rather than back off.
 *   <li><b>{@code cluster.health}</b> is answered from local state, because it is the first thing anyone runs and
 *       hanging on it makes a working node look dead.
 *   <li><b>{@code internal:} actions are left completely alone.</b> That is the safety rule that matters here:
 *       the server's own machinery — shard-started notifications through {@code LocalShardStateAction}, peer
 *       recovery, anything else that happens to travel as a cluster-manager request — must keep working, and this
 *       filter has no business guessing about traffic no user sent.
 * </ul>
 *
 * <p>Lifted in shape from {@code cluster-etcd}'s filter of the same name, which reached the same three
 * conclusions. The bodies differ: the message names the control plane, because in this design there is somewhere
 * specific for a rejected mutation to go.
 *
 * <p><b>Whose view.</b> Answering {@code cluster.health} and {@code cluster.state} from local state gives a
 * node-local view — one node, its own shards. That is what this node answers when a request carries no
 * {@code x-tenant-id}: the platform team's view, for debugging. A tenant's view of the cluster is the fleet's, which
 * the supervisor projects into {@code tenant/<t>/status/cluster} and {@code tenant/<t>/status/indices/<uuid>}. Serving
 * those is the control plane's, since it is only a read of the store and the control plane is stateless, so a request
 * carrying {@code x-tenant-id} is refused here with a reason saying so.
 */
public class ClusterManagerActionFilter implements ActionFilter {

    private static final Logger logger = LogManager.getLogger(ClusterManagerActionFilter.class);

    /** Actions the server sends to itself. Not a user's intent, and not this filter's business. */
    private static final String INTERNAL_PREFIX = "internal:";

    /** The one that produced the confusing failure, and the one worth naming specifically. */
    private static final String PUT_MAPPING = "indices:admin/mapping/put";

    private static final String AUTO_PUT_MAPPING = "indices:admin/mapping/auto_put";

    private final java.util.function.Supplier<ClusterState> state;
    private final String clusterName;
    private final java.util.function.Supplier<String> tenantHeader;

    /**
     * Takes the things it needs rather than a {@code ClusterService}, which is the difference between a class
     * that can be tested and one that needs a node to instantiate.
     */
    public ClusterManagerActionFilter(java.util.function.Supplier<ClusterState> state, String clusterName) {
        this(state, clusterName, () -> null);
    }

    /**
     * @param tenantHeader the request's {@code x-tenant-id}, read from the thread context; null or blank when it has none
     */
    public ClusterManagerActionFilter(
        java.util.function.Supplier<ClusterState> state,
        String clusterName,
        java.util.function.Supplier<String> tenantHeader
    ) {
        this.state = state;
        this.clusterName = clusterName;
        this.tenantHeader = tenantHeader;
    }

    @Override
    public int order() {
        return 0;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <Request extends ActionRequest, Response extends ActionResponse> void apply(
        Task task,
        String action,
        Request request,
        ActionListener<Response> listener,
        ActionFilterChain<Request, Response> chain
    ) {
        if (action.startsWith(INTERNAL_PREFIX)) {
            chain.proceed(task, action, request, listener);
            return;
        }

        if (action.equals(ClusterHealthAction.NAME) || action.equals(ClusterStateAction.NAME)) {
            String tenant = tenantHeader.get();
            if (tenant != null && tenant.isBlank() == false) {
                // A tenant asking about the cluster. This node holds one part of the fleet, and its own health and state
                // describe that part, every tenant's indices on it included. The tenant's view of the cluster is built
                // from the supervisor's projections, which is the control plane's to serve, so it is refused here, the
                // way a mutation is: at once, with somewhere to go. Without the header, this node answers with its own
                // view, which is what the platform team debugging it needs.
                listener.onFailure(
                    new OpenSearchStatusException(
                        "["
                            + action
                            + "] for tenant ["
                            + tenant
                            + "] is the control plane's to answer: a clusterless node holds one part of the fleet, and "
                            + "its own health and state describe that part. Send the request to the control plane. (Without "
                            + "x-tenant-id, this node answers with its own local view.)",
                        RestStatus.BAD_REQUEST
                    )
                );
                return;
            }
        }

        if (action.equals(ClusterHealthAction.NAME)) {
            ClusterState current = state.get();
            ClusterHealthResponse health = new ClusterHealthResponse(
                clusterName,
                current.metadata().getConcreteAllIndices(),
                current
            );
            ((ActionListener<ClusterHealthResponse>) listener).onResponse(health);
            return;
        }

        if (request instanceof ClusterManagerNodeReadRequest<?> read) {
            // Answered from this node's state rather than routed to a manager that does not exist.
            read.local(true);
            chain.proceed(task, action, request, listener);
            return;
        }

        if (request instanceof ClusterManagerNodeRequest<?>) {
            listener.onFailure(new OpenSearchStatusException(refusalFor(action), RestStatus.BAD_REQUEST));
            return;
        }

        chain.proceed(task, action, request, listener);
    }

    /**
     * Why the action was refused, in terms of what to do instead.
     *
     * <p>The mapping cases get their own sentence because they are reached indirectly: nobody asks for
     * {@code auto_put}, they index a document, and a message about a cluster manager would leave them looking in
     * the wrong place entirely.
     */
    private static String refusalFor(String action) {
        if (action.equals(AUTO_PUT_MAPPING)) {
            logger.debug("clusterless: refused a dynamic mapping update");
            return "This write needs a mapping change, and a clusterless node cannot make one: mappings are "
                + "desired state and belong to the control plane. Put the field in the index's mappings there, "
                + "then index the document. (Refused ["
                + action
                + "] locally rather than waiting for a cluster manager that does not exist.)";
        }
        if (action.equals(PUT_MAPPING)) {
            return "Mappings are desired state and belong to the control plane, which writes them where the "
                + "supervisor and this node read them. Send put_mapping there instead of to a data node. "
                + "(Refused ["
                + action
                + "] locally.)";
        }
        return "["
            + action
            + "] changes cluster metadata, which a clusterless node has no way to publish. Metadata is the "
            + "control plane's: send this there instead. Refused locally rather than waiting for a cluster "
            + "manager that does not exist.";
    }
}
