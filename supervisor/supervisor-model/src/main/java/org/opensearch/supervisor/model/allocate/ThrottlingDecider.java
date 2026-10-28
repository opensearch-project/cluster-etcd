package org.opensearch.supervisor.model.allocate;

import org.opensearch.supervisor.model.NodeId;

/**
 * Limits how many copies one node may be recovering at once.
 *
 * <p>Paces the work rather than forbidding it, which is why it answers {@code THROTTLE} and not {@code NO}. A
 * node told to recover fifty shards at once recovers all of them slowly and serves badly meanwhile; the same
 * fifty in batches finish sooner and the node stays usable throughout. The difference matters most in exactly
 * the situation where the supervisor has most to do — a node has just come back, or a new one has joined, and
 * there is a large backlog aimed at it.
 *
 * <p>Distinct from the planner's work ceiling, which bounds a <em>pass</em> across the whole pool. This bounds
 * one node, which is where the cost actually lands.
 */
public final class ThrottlingDecider implements AllocationDecider {

    public static final String NAME = "throttling";

    public static final int DEFAULT_CONCURRENT_RECOVERIES = 2;

    private final int concurrentRecoveries;

    public ThrottlingDecider() {
        this(DEFAULT_CONCURRENT_RECOVERIES);
    }

    public ThrottlingDecider(int concurrentRecoveries) {
        if (concurrentRecoveries < 1) {
            throw new IllegalArgumentException(
                "at least one concurrent recovery must be allowed, or nothing would ever be placed"
            );
        }
        this.concurrentRecoveries = concurrentRecoveries;
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public Decision canAllocate(AllocationContext context, NodeId candidate) {
        int recovering = context.recoveringOn(candidate);
        if (recovering >= concurrentRecoveries) {
            return Decision.throttle(NAME, "recovering", "node [" + candidate + "] is already recovering " + recovering
                + " copies, at the limit of " + concurrentRecoveries + "; this is pacing, not a refusal");
        }
        return Decision.yes(NAME, "node [" + candidate + "] is recovering " + recovering + " of at most "
            + concurrentRecoveries + " copies");
    }
}
