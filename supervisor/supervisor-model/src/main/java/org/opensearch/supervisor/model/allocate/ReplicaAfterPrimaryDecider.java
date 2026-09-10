package org.opensearch.supervisor.model.allocate;

import org.opensearch.supervisor.model.NodeId;
import org.opensearch.supervisor.model.node.ShardRole;
import org.opensearch.supervisor.model.status.Replication;

/**
 * A document-replication replica is not placed until its primary is serving.
 *
 * <p>This is {@code DESIGN.md} open question 6, closed — and a decider is the right shape for it, because the
 * rule is a property of a candidate placement rather than a phase of the planner. A docrep replica resolves its
 * primary from {@code tenant/<t>/status/indices/<uuid>} and cannot recover until one is serving, so a plan that
 * places replicas first converges only by accident, and not at all if the work ceiling keeps cutting the pass
 * before the primary is reached.
 *
 * <p><b>Segment replication is exempt</b>, which is why the rule is per index rather than global: a segrep
 * replica syncs from the remote store and never contacts the primary, so there is nothing for it to wait on.
 */
public final class ReplicaAfterPrimaryDecider implements AllocationDecider {

    public static final String NAME = "replica_after_primary";

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public Decision canAllocate(AllocationContext context, NodeId candidate) {
        if (context.role() == ShardRole.PRIMARY) {
            return Decision.yes(NAME, "this is a primary, so nothing precedes it");
        }
        if (context.index().replication() != Replication.DOCUMENT) {
            return Decision.yes(NAME, "index [" + context.index().name() + "] uses segment replication, so a"
                + " replica syncs from the remote store and has no primary to wait for");
        }
        if (context.primaryIsActive() == false) {
            return Decision.throttle(NAME, "index [" + context.index().name() + "] shard " + context.shard()
                + " has no serving primary yet, and a document-replication replica cannot recover without one;"
                + " this is a wait rather than a refusal");
        }
        return Decision.yes(NAME, "the primary of shard " + context.shard() + " is serving");
    }
}
