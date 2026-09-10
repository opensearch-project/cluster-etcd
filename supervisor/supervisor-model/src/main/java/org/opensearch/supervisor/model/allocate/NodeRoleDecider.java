package org.opensearch.supervisor.model.allocate;

import org.opensearch.supervisor.model.NodeId;
import org.opensearch.supervisor.model.node.NodeRole;

/**
 * Only a node with the {@code data} role takes shard copies.
 *
 * <p>Obvious, and it was missing: {@code Observation.NodeObservation#isPlaceable} checks that a node is
 * registered, pooled and live but says nothing about what it is <em>for</em>, so a coordinator-only node was a
 * perfectly good placement target. It would have accepted the assignment and been unable to do anything with
 * it, and the shard would have sat unrecovered with the diff insisting it was placed.
 */
public final class NodeRoleDecider implements AllocationDecider {

    public static final String NAME = "node_role";

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public Decision canAllocate(AllocationContext context, NodeId candidate) {
        var node = context.observation().nodes().get(candidate);
        if (node == null || node.registration() == null) {
            return Decision.no(NAME, "node [" + candidate + "] has no registration, so its roles are unknown");
        }
        if (node.registration().hasRole(NodeRole.DATA) == false) {
            return Decision.no(NAME, "node [" + candidate + "] declares roles " + node.registration().roles()
                + " and not data, so it cannot hold a shard copy");
        }
        return Decision.yes(NAME, "node [" + candidate + "] is a data node");
    }
}
