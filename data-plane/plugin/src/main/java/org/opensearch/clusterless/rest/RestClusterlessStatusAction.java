/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.clusterless.rest;

import org.opensearch.clusterless.apply.AssignmentPoller;
import org.opensearch.clusterless.publish.NodePublisher;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestRequest;
import org.opensearch.transport.client.node.NodeClient;

import java.util.List;
import java.util.function.Supplier;

/**
 * {@code GET /_clusterless/status} — what this node last read, last applied, and last wrote.
 *
 * <p>D7.3, and the requirement is more specific than it looks: the supervisor's projections answer "what
 * does the fleet look like", and this answers "why is <em>this</em> node behaving like that". The second
 * question is the one asked at 3am, and the store cannot answer it — a node that is failing to publish is
 * precisely a node whose story is not in the store.
 *
 * <p>Deliberately node-local: it goes to whichever node serves the request and reports on that node, with
 * no fan-out. A fan-out would need a transport action and a node list, and the node list is the thing this
 * architecture does not have.
 *
 * <p>Two parts of the output are worth explaining to whoever reads it.
 *
 * <p>{@code inventory.generated_at} and {@code inventory.written_at} differ on purpose: the first is when the
 * copies last changed and the second is when the document was last put into the store. A large gap between them
 * is the system working — the inventory is deliberately not rewritten while nothing has changed — and it is the
 * fastest way to confirm that from outside.
 *
 * <p>{@code assignment.deferred} is the list of copies this node was told to hold and did not build, with a
 * reason for each. It is the answer to the most confusing question this architecture can produce: the supervisor
 * reports a shard as unassigned, the node looks healthy, and nothing anywhere says that the node considered the
 * shard and declined it. Usually the reason is ordinary and temporary — an index document that has not landed —
 * and it should be empty on a converged node.
 */
public class RestClusterlessStatusAction extends BaseRestHandler {

    private final Supplier<NodePublisher> publisher;
    private final Supplier<AssignmentPoller> poller;

    public RestClusterlessStatusAction(Supplier<NodePublisher> publisher, Supplier<AssignmentPoller> poller) {
        this.publisher = publisher;
        this.poller = poller;
    }

    @Override
    public String getName() {
        return "clusterless_status";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(RestRequest.Method.GET, "/_clusterless/status"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        NodePublisher currentPublisher = publisher.get();
        AssignmentPoller currentPoller = poller.get();
        return channel -> {
            XContentBuilder builder = channel.newBuilder();
            builder.startObject();
            if (currentPublisher == null) {
                // Between the plugin being constructed and the node finishing its start. Saying so beats
                // a 500, and beats an empty object that looks like a node holding nothing.
                builder.field("publishing", "not started");
            } else {
                write(builder, currentPublisher.status());
            }
            if (currentPoller != null) {
                write(builder, currentPoller.status());
            }
            builder.endObject();
            channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
        };
    }

    private static void write(XContentBuilder builder, AssignmentPoller.PollStatus status) throws java.io.IOException {
        builder.startObject("assignment");
        builder.field("apply_enabled", status.applyEnabled());
        builder.field("assignments_read", status.assignmentsRead());
        builder.field("version", status.assignmentVersion());
        builder.field("indices", status.indices());
        builder.field("copies", status.copies());
        if (status.coordinatorTenants().isEmpty() == false) {
            builder.field("coordinating_for", status.coordinatorTenants());
        }
        if (status.coordinatedIndices() > 0) {
            // Indices whose other copies this node knows about, for either reason -- coordinating a tenant, or
            // holding a copy and needing the peers of its own shards. `serving_copies` is the number that matters:
            // zero of those with everything else healthy is a node that answers with nothing and looks fine doing
            // it, and for a node holding a copy it means a primary with nobody to publish checkpoints to.
            builder.startObject("peers");
            builder.field("indices", status.coordinatedIndices());
            builder.field("serving_copies", status.coordinatedServingCopies());
            builder.endObject();
        }
        builder.field("deferred", status.deferred());
        if (status.lastFailure() != null) {
            builder.field("last_failure", status.lastFailure());
        }
        builder.endObject();
    }

    private static void write(XContentBuilder builder, NodePublisher.PublishStatus status) throws java.io.IOException {
        builder.field("node_id", status.nodeId().value());
        builder.field("ephemeral_id", status.ephemeralId());

        builder.startObject("registration");
        builder.field("published", status.registered());
        if (status.registered()) {
            builder.field("registered_at", status.registeredAt());
        }
        // Rendered only when it has happened, so its presence is the signal. Something removing a live node's
        // registration is not ordinary, and a node that healed itself every pass would otherwise leave no trace.
        if (status.registrationRewrites() > 0) {
            builder.field("rewrites_after_going_missing", status.registrationRewrites());
        }
        builder.endObject();

        builder.startObject("inventory");
        builder.field("published", status.inventoryCopies() >= 0);
        if (status.inventoryCopies() >= 0) {
            builder.field("copies", status.inventoryCopies());
            builder.field("digest", status.inventoryDigest());
            builder.field("generated_at", status.inventoryGeneratedAt());
            builder.field("written_at", status.inventoryWrittenAt());
        }
        builder.endObject();

        builder.startObject("heartbeat");
        builder.field("written_at", status.heartbeatWrittenAt());
        builder.endObject();

        builder.startObject("applied");
        builder.field("assignment_version", status.appliedAssignmentVersion());
        builder.field("outcome", status.appliedOutcome().wireName());
        builder.endObject();

        builder.startObject("store");
        builder.field("consecutive_failed_passes", status.consecutiveFailures());
        if (status.lastFailure() != null) {
            builder.field("last_failure", status.lastFailure());
        }
        builder.endObject();
    }
}
