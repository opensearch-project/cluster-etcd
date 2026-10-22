package org.opensearch.supervisor.model.allocate;

import org.opensearch.supervisor.model.NodeId;
import org.opensearch.supervisor.model.deploy.DeploymentCoverage;
import org.opensearch.supervisor.model.node.ShardRole;

/**
 * Keeps new copies off nodes a deployment is taking away ({@code DESIGN.md} §29.3), as upstream's
 * {@code DeploymentAllocationDecider} keeps primaries off a draining node.
 *
 * <ul>
 *   <li><b>A drain whose nodes return</b> takes no new primary. A replica may still be placed there, as upstream allows:
 *       the node is coming back, and its copies keep counting while it is away.
 *   <li><b>A drain that removes its nodes</b> takes no new copy of any role, since everything on it is moving off.
 *   <li><b>A frozen node</b>, covered by a deployment that cannot be acted on as written, or by two, or in a pool
 *       with one that does not parse, takes no new primary. Freezing holds what may be protected, and a primary is
 *       what a drain protects (§29.10).
 * </ul>
 *
 * <p>An expired drain is not in force, and refuses nothing. The dual-run states are not judged here yet: where a
 * dual-run's new primary may go is the dual-run's own rule, and nothing places one.
 */
public final class DeploymentDecider implements AllocationDecider {

    public static final String NAME = "deployment";

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public Decision canAllocate(AllocationContext context, NodeId candidate) {
        DeploymentCoverage.Covered covered = DeploymentCoverage.ofNode(context.observation(), candidate);
        if (covered == null) {
            return Decision.yes(NAME, "node [" + candidate + "] is under no deployment");
        }
        boolean primary = context.role() == ShardRole.PRIMARY;
        if (covered.removing()) {
            return Decision.no(NAME, "removing", "node [" + candidate + "] is being removed by deployment [" + covered.id()
                + "], so every copy is moving off it and none is placed there");
        }
        if (covered.draining() && primary) {
            return Decision.no(NAME, "draining", "node [" + candidate + "] is being drained by deployment [" + covered.id()
                + "], which takes its primaries elsewhere; a new one is not placed there");
        }
        if (covered.frozen() && primary) {
            return Decision.no(NAME, "frozen", "node [" + candidate + "] is frozen, because "
                + (covered.id() == null ? "a deployment in this pool cannot be parsed"
                    : "deployment [" + covered.id() + "] cannot be acted on as written, or another also selects it")
                + "; it takes no new primary until the deployment system corrects it");
        }
        return Decision.yes(NAME, "node [" + candidate + "] may take this copy under deployment ["
            + (covered.id() == null ? "unknown" : covered.id()) + "]");
    }
}
