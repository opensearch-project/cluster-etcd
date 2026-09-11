package org.opensearch.supervisor.model.balance;

import org.opensearch.supervisor.model.allocate.Decision;
import org.opensearch.supervisor.model.status.Replication;

/**
 * Refuses to move a primary, because this service cannot yet do it safely.
 *
 * <p>Not a policy preference — a missing mechanism, and a decider rather than a hidden {@code if} so that the
 * gap is listed, explained to whoever asks why their primaries are not moving, and deleted in one place on the
 * day it closes.
 *
 * <p>Moving a primary means there is an interval with two nodes believing they are the primary, or none. Under
 * segment replication to a remote store, two primaries write to the same remote path, which can corrupt the
 * data; the fix is a per-primary generation so a superseded primary can be fenced off, and that is work in
 * OpenSearch itself rather than here. Under document replication the interval has to be a gap rather than an
 * overlap, with the primary term incremented across it, which needs a term this service does not yet keep.
 *
 * <p>Both readings are refusals today, and the refusal says which one applies, because they close on different
 * schedules and an operator tracking the gap needs to know which one they are waiting for.
 *
 * <p>The cost of the gap, stated plainly: a node loaded mostly with primaries cannot be relieved. Its replicas
 * will move away and it will keep the primaries. Draining such a node is a manual operation until this lifts.
 */
public final class PrimaryMoveDecider implements RebalanceDecider {

    public static final String NAME = "primary_move";

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public Decision canRebalance(RebalanceContext context) {
        if (context.isPrimary() == false) {
            return Decision.yes(NAME, "this is a replica, which can be built beside the original and then"
                + " dropped, so no node ever has to stop being the authority for anything");
        }
        if (context.replication() == Replication.SEGMENT) {
            return Decision.no(NAME, "moving a primary is not implemented: with segment replication to a remote"
                + " store, two nodes briefly believing they are primary would write to the same remote path,"
                + " which can corrupt the shard, and fencing the old one needs a per-primary generation that"
                + " OpenSearch does not yet expose (DESIGN.md §13, open question 1)");
        }
        return Decision.no(NAME, "moving a primary is not implemented: with document replication the handover"
            + " needs an interval with no primary at all and the primary term incremented across it, and this"
            + " service does not yet keep a primary term (DESIGN.md §13, open question 1)");
    }
}
