package org.opensearch.supervisor.model.plan;

import org.opensearch.supervisor.model.NodeId;
import org.opensearch.supervisor.model.TenantId;
import org.opensearch.supervisor.model.diff.Subject;
import org.opensearch.supervisor.model.node.ShardRole;

/**
 * One thing to do.
 *
 * <p>Every instruction carries its {@code because}, for the same reason a finding does: R7.4 asks for the
 * decision rather than the action, and "placed shard 0 on node A" without the clause that follows it is not
 * debuggable.
 */
public sealed interface Instruction {

    Subject subject();

    String because();

    /**
     * Place a copy of a shard on a node.
     *
     * <p>The safe half of the pair: the worst a wrong placement can do is create a surplus copy, which is a
     * self-clearing finding rather than lost data.
     *
     * @param configRevision the revision of the index document this placement was decided from, passed to
     *     the node so it can tell whether it is building against the configuration the supervisor intended
     */
    record PlaceCopy(
        TenantId tenant,
        String indexUuid,
        String indexName,
        int shard,
        ShardRole role,
        NodeId node,
        long configRevision,
        String because
    ) implements Instruction {

        @Override
        public Subject subject() {
            return Subject.ofShard(tenant, indexUuid, shard);
        }
    }

    /**
     * Drop a copy of a shard from a node, by removing it from that node's assignment.
     *
     * <p><b>The only destructive instruction, and the reason it is safe is that it is never paired with an
     * addition.</b> A move is not expressed here as "add there, remove here" — it is expressed as an addition
     * now, and then, one or more passes later, an independent decision that some shard has more started copies
     * than it wants and that this is the one to drop. Nothing remembers that a move was in progress.
     *
     * <p>That indirection is the point, and it is what {@code DESIGN.md} §6's add-before-remove rule becomes
     * once reconciliation is level-triggered. A supervisor that held "I am moving shard 3 from A to B" in memory
     * would have to decide what to do when it crashed between the halves, when its lease moved to a peer
     * mid-move, or when the target failed to build the copy — and each of those is a way to remove the old copy
     * with nothing to replace it. Deriving the removal from observed state instead means there is no in-between
     * to recover: at every instant either the pool has a surplus copy, which is untidy and safe, or it does not.
     *
     * <p>Three consequences follow, and they are properties rather than precautions:
     *
     * <ul>
     *   <li>A removal is only ever proposed when the copies that would remain are <b>already started</b>, so
     *       redundancy never dips below what the index asked for — not even momentarily.
     *   <li>A crashed supervisor leaves a surplus copy, which the next holder of the lease clears by the same
     *       rule. Nothing is lost and nothing is stuck.
     *   <li>If the target never manages to build its copy, the surplus never appears and the original is never
     *       dropped. A failed move costs bandwidth, not availability.
     * </ul>
     *
     * <p>The node is told by <em>omission</em> — the assignment is rewritten without this shard — which is the
     * same shape as every other assignment write and means the data plane needs no new verb. What the node does
     * with the local data is its own business; the supervisor's statement is only that it should no longer be
     * serving this copy.
     */
    record RemoveCopy(
        TenantId tenant,
        String indexUuid,
        int shard,
        NodeId node,
        String because
    ) implements Instruction {

        @Override
        public Subject subject() {
            return Subject.ofShard(tenant, indexUuid, shard);
        }
    }
}
