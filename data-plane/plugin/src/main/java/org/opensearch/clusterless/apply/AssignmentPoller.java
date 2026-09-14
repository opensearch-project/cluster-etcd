/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.clusterless.apply;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.ClusterState;
import org.opensearch.clusterless.contract.AppliedOutcome;
import org.opensearch.clusterless.contract.Assignment;
import org.opensearch.clusterless.contract.Heartbeat;
import org.opensearch.clusterless.contract.Keys;
import org.opensearch.clusterless.contract.MalformedDocumentException;
import org.opensearch.clusterless.contract.NodeId;
import org.opensearch.clusterless.publish.NodePublisher;
import org.opensearch.controlplane.store.KeyValueStore;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Reads this node's assignment, works out what it would do, and — at this milestone — says so without doing it.
 *
 * <p>Read-only first, mirroring the supervisor's own default and for the same reason: this is the component that
 * can move data, and the cheapest way to introduce one of those is to have it explain itself for a while where
 * somebody can check the explanation. {@code clusterless.apply} turns it into action.
 *
 * <h2>Level-triggered, and what that costs</h2>
 *
 * <p>Every pass re-reads the assignment and every index document it names, and rebuilds the intended state from
 * what it read. Nothing here remembers what changed, because a decision derived from a change is a decision
 * that is wrong after a missed one. The only state kept between passes is a copy of the last intended state, and
 * it is used for exactly one thing: deciding whether to log. Convergence does not consult it.
 *
 * <h2>Absent is not empty</h2>
 *
 * <p>The single most destructive misreading available to this class is treating a missing assignment as an empty
 * one, because an empty assignment means "drop every shard you hold". A supervisor that has not written one yet,
 * a pool between owners and a lost key all look exactly like that. So an absent key leaves everything alone and
 * does not even touch the applied ack, and an unparseable one is reported as a failure rather than obeyed.
 */
public final class AssignmentPoller {

    private static final Logger logger = LogManager.getLogger(AssignmentPoller.class);

    private final KeyValueStore store;
    private final NodeId nodeId;
    private final DesiredStateResolver resolver;
    private final Supplier<ClusterState> currentState;
    private final Function<DesiredState, NodeState> nodeStates;
    private final NodePublisher publisher;
    private final boolean applyEnabled;

    private DesiredState lastResolved = DesiredState.nothing();
    private String lastReport;
    private long assignmentsRead;
    private String lastFailure;

    public AssignmentPoller(
        KeyValueStore store,
        NodeId nodeId,
        DesiredStateResolver resolver,
        Supplier<ClusterState> currentState,
        Function<DesiredState, NodeState> nodeStates,
        NodePublisher publisher,
        boolean applyEnabled
    ) {
        this.store = Objects.requireNonNull(store, "store");
        this.nodeId = Objects.requireNonNull(nodeId, "nodeId");
        this.resolver = Objects.requireNonNull(resolver, "resolver");
        this.currentState = Objects.requireNonNull(currentState, "currentState");
        this.nodeStates = Objects.requireNonNull(nodeStates, "nodeStates");
        this.publisher = Objects.requireNonNull(publisher, "publisher");
        this.applyEnabled = applyEnabled;
    }

    /**
     * One pass. Never throws, for the reason the publishing pass does not: an exception out of a scheduled task
     * stops it being rescheduled on some executors, and a node that quietly stopped reading its assignment would
     * look healthy while ignoring every instruction it was given.
     */
    public synchronized void poll() {
        try {
            Optional<KeyValueStore.Versioned> stored = store.get(Keys.nodeAssignment(nodeId));
            if (stored.isEmpty()) {
                // No instruction. Not an empty desired state -- see the class comment -- so nothing is
                // resolved, nothing is reported, and the last applied ack stands.
                logger.debug("clusterless: no assignment for this node yet");
                lastFailure = null;
                return;
            }

            Assignment assignment;
            try {
                assignment = Assignment.read(stored.get().value());
            } catch (MalformedDocumentException e) {
                // Reported, and changes nothing. Reading a parse failure as an empty assignment would let one
                // corrupt write empty a node.
                lastFailure = e.getMessage();
                logger.error("clusterless: {}; ignoring it and keeping the last assignment", e.getMessage());
                publisher.reportApplied(
                    new Heartbeat.Applied(lastResolved.assignmentVersion(), AppliedOutcome.FAILED, e.getMessage())
                );
                return;
            }

            assignmentsRead++;
            lastFailure = null;
            DesiredState desired = resolver.resolve(assignment);
            lastResolved = desired;

            ClusterState intended = nodeStates.apply(desired).buildClusterState(currentState.get());
            report(intended, desired);
            publisher.reportApplied(ackFor(desired));
        } catch (Exception e) {
            lastFailure = e.getClass().getSimpleName() + ": " + e.getMessage();
            logger.warn("clusterless: could not read this node's assignment; keeping the last one", e);
        }
    }

    /**
     * What to tell the supervisor.
     *
     * <p>In read-only mode the honest answer is {@code failed}, and the alternatives are both worse.
     * {@code applied} would be a lie the supervisor acts on — it would stop expecting anything to change while
     * this node did nothing. A permanent {@code applying} would imply progress that is not coming. {@code failed}
     * with a reason produces a finding, which is exactly what a node that has been told to hold shards and is
     * configured not to should produce.
     *
     * <p>The version is still echoed, because "which assignment this refers to" is useful in every case.
     */
    private Heartbeat.Applied ackFor(DesiredState desired) {
        if (applyEnabled == false) {
            String detail = "read-only: [clusterless.apply] is false, so nothing was applied"
                + (desired.deferred().isEmpty() ? "" : "; " + desired.deferred().size() + " copy(ies) would also have been deferred");
            return new Heartbeat.Applied(desired.assignmentVersion(), AppliedOutcome.FAILED, detail);
        }
        // Milestone 4 replaces this. Applying is not implemented, so claiming anything else would be a lie
        // with a different shape.
        return new Heartbeat.Applied(desired.assignmentVersion(), AppliedOutcome.FAILED, "applying is not implemented yet");
    }

    /**
     * Logs the intended state when it differs from the last one logged.
     *
     * <p>At {@code INFO}, because in read-only mode this is the output the milestone exists to produce. Only on
     * change, because it is several hundred bytes per index and the poll interval is seconds — a node with fifty
     * indices would otherwise write a megabyte an hour saying nothing had happened.
     */
    private void report(ClusterState intended, DesiredState desired) {
        String report = ClusterStateReport.describe(intended, desired);
        if (report.equals(lastReport)) {
            logger.debug("clusterless: intended state unchanged at assignment version {}", desired.assignmentVersion());
            return;
        }
        lastReport = report;
        if (applyEnabled) {
            logger.info("clusterless: applying\n{}", report);
        } else {
            logger.info("clusterless: read-only, so the following was NOT applied\n{}", report);
        }
    }

    /** What this node last read, for the local diagnostic D7.3 asks for. */
    public synchronized PollStatus status() {
        return new PollStatus(
            applyEnabled,
            assignmentsRead,
            lastResolved.assignmentVersion(),
            lastResolved.indices().size(),
            lastResolved.copyCount(),
            lastResolved.coordinatorTenants().stream().map(Object::toString).toList(),
            lastResolved.deferred(),
            lastFailure
        );
    }

    /**
     * A snapshot of the reading half of a pass.
     *
     * <p>{@code deferred} is the field worth having: "the supervisor asked for a shard and this node did not
     * build it" is otherwise invisible from both ends — the store shows an unassigned shard and the node shows
     * nothing at all.
     */
    public record PollStatus(
        boolean applyEnabled,
        long assignmentsRead,
        long assignmentVersion,
        int indices,
        int copies,
        List<String> coordinatorTenants,
        List<String> deferred,
        String lastFailure
    ) {}
}
