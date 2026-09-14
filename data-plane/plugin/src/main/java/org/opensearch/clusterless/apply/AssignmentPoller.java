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
 * Reads this node's assignment, works out what to do, and does it — or, with {@code clusterless.apply} false,
 * says what it would have done.
 *
 * <p>Read-only is the default, mirroring the supervisor's own and for the same reason: this is the component that
 * can move data, and the cheapest way to introduce one of those is to have it explain itself for a while where
 * somebody can check the explanation.
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
    private final ClusterStateApplier applier;
    private final boolean applyEnabled;

    private DesiredState lastResolved = DesiredState.nothing();
    private String lastReport;
    private long assignmentsRead;
    private long statesApplied;
    private String lastFailure;
    private String lastApplyFailure;

    /**
     * @param applier where a fabricated state goes when applying is enabled. May be {@code null} in read-only
     *     mode, which is what makes the read-only path runnable without a node behind it.
     */
    public AssignmentPoller(
        KeyValueStore store,
        NodeId nodeId,
        DesiredStateResolver resolver,
        Supplier<ClusterState> currentState,
        Function<DesiredState, NodeState> nodeStates,
        NodePublisher publisher,
        ClusterStateApplier applier,
        boolean applyEnabled
    ) {
        this.store = Objects.requireNonNull(store, "store");
        this.nodeId = Objects.requireNonNull(nodeId, "nodeId");
        this.resolver = Objects.requireNonNull(resolver, "resolver");
        this.currentState = Objects.requireNonNull(currentState, "currentState");
        this.nodeStates = Objects.requireNonNull(nodeStates, "nodeStates");
        this.publisher = Objects.requireNonNull(publisher, "publisher");
        this.applier = applier;
        this.applyEnabled = applyEnabled;
        if (applyEnabled && applier == null) {
            throw new IllegalArgumentException("applying is enabled but no applier was given");
        }
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

            ClusterState live = currentState.get();
            // Fabricated against the live state to decide whether anything needs doing, and fabricated again
            // inside the applier against whatever the server hands over. The two can differ -- a shard may have
            // finished recovering in between -- and the applier's copy is the one that counts.
            ClusterState intended = nodeStates.apply(desired).buildClusterState(live);

            if (StateEquivalence.sameContent(live, intended)) {
                // The fixed point. Applying here would publish a state identical to the running one except for
                // its version, which costs every applier and listener in the server a pass and says nothing.
                logger.debug(
                    "clusterless: already holding what assignment version {} asks for; nothing to apply",
                    desired.assignmentVersion()
                );
            } else {
                report(intended, desired);
                if (applyEnabled) {
                    applier.apply(
                        "clusterless assignment version " + desired.assignmentVersion(),
                        previous -> nodeStates.apply(desired).buildClusterState(previous),
                        this::applied
                    );
                }
            }
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
        if (lastApplyFailure != null) {
            return new Heartbeat.Applied(desired.assignmentVersion(), AppliedOutcome.FAILED, lastApplyFailure);
        }

        // Measured from the live state rather than from the fact that an update was submitted. Every new copy
        // is INITIALIZING at the moment it is applied, so "the state went in" and "the node holds what it was
        // told to" are different claims and only the second one is worth making.
        Convergence convergence = Convergence.assess(desired, currentState.get(), nodeId.value());
        List<String> notBuilt = desired.deferred();
        if (convergence.isComplete() && notBuilt.isEmpty()) {
            return new Heartbeat.Applied(desired.assignmentVersion(), AppliedOutcome.APPLIED, null);
        }

        String detail = convergence.detail();
        if (notBuilt.isEmpty() == false) {
            // Copies this node declined are not something more time will fix, so they are reported as a failure
            // even while the rest is still converging. The supervisor should see that part as stuck.
            String declined = notBuilt.size() + " copy(ies) not built: " + notBuilt.get(0);
            return new Heartbeat.Applied(
                desired.assignmentVersion(),
                AppliedOutcome.FAILED,
                detail == null ? declined : detail + "; " + declined
            );
        }
        return new Heartbeat.Applied(desired.assignmentVersion(), AppliedOutcome.APPLYING, detail);
    }

    /**
     * The applier's callback, which arrives on the server's thread rather than a pass's.
     *
     * <p>Only records. The next pass computes the ack from the live state, so there is nothing to publish from
     * here — and publishing from here would write a heartbeat from an unexpected thread, out of order with the
     * pass that owns that document.
     */
    private synchronized void applied(String source, Exception failure) {
        if (failure == null) {
            statesApplied++;
            lastApplyFailure = null;
            logger.info("clusterless: applied {}", source);
        } else {
            lastApplyFailure = "could not apply " + source + ": " + failure;
            logger.error("clusterless: could not apply {}", source, failure);
        }
    }

    /**
     * Logs the intended state, at {@code INFO}, when there is something to say.
     *
     * <p>Only reached when the intended state differs from what is applied, which is what makes the line worth
     * reading: it appears when this node is about to change something. It used to be guarded by comparing the
     * rendered report with the last one, which did not work — the report names the cluster state version, so it
     * differed every pass and logged several hundred bytes per index per second. Comparing states rather than
     * their descriptions is both correct and the thing that decides whether to apply, so there is now one
     * decision instead of two that could disagree.
     *
     * <p>The last report is still remembered, for the one case the state comparison cannot see: in read-only
     * mode nothing is ever applied, so every pass differs from the live state for ever. Without this the
     * read-only node would be the noisy one.
     */
    private void report(ClusterState intended, DesiredState desired) {
        String report = ClusterStateReport.describe(intended, desired);
        String withoutVersion = report.substring(report.indexOf(','));
        if (withoutVersion.equals(lastReport)) {
            logger.debug("clusterless: intended state unchanged at assignment version {}", desired.assignmentVersion());
            return;
        }
        lastReport = withoutVersion;
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
            statesApplied,
            lastResolved.assignmentVersion(),
            lastResolved.indices().size(),
            lastResolved.copyCount(),
            lastResolved.coordinatorTenants().stream().map(Object::toString).toList(),
            lastResolved.deferred(),
            lastFailure,
            lastApplyFailure
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
        long statesApplied,
        long assignmentVersion,
        int indices,
        int copies,
        List<String> coordinatorTenants,
        List<String> deferred,
        String lastFailure,
        String lastApplyFailure
    ) {}
}
