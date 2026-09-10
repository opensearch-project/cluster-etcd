package org.opensearch.supervisor.model.plan;

import org.opensearch.supervisor.model.NodeId;
import org.opensearch.supervisor.model.TenantId;
import org.opensearch.supervisor.model.diff.Subject;
import org.opensearch.supervisor.model.node.ShardRole;

/**
 * One thing to do. A sealed interface with a single case for now, because milestone 5 acts on exactly one
 * finding and adding the rest is a change to the planner rather than to this file's shape.
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
     * <p>Only ever an <b>addition</b> in this milestone. Nothing removes a copy yet, which sidesteps the
     * whole class of destructive mistakes: the worst a wrong instruction can do is create a surplus copy,
     * which is a self-clearing finding rather than lost data. {@code DESIGN.md} §6's add-before-remove rule
     * is trivially satisfied while there is no remove.
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
}
