package org.opensearch.supervisor.model.balance;

import org.opensearch.supervisor.model.allocate.Decision;
import org.opensearch.supervisor.model.deploy.DeploymentCoverage;

/**
 * No rebalancing in a pool with a deployment in progress ({@code DESIGN.md} §29.3), nor in one with anything frozen.
 *
 * <p>Load readings are wrong in both directions while a deployment shifts traffic, and copies are already moving for a
 * reason the balancer does not know; a move made now would be undone or fought. So it waits, which is why this answers
 * {@code THROTTLE} rather than {@code NO}: it is "not now", and the deployment's end is the "now". The same holds for a
 * node frozen by a deployment that cannot be acted on, which must be left as it is.
 */
public final class DeploymentRebalanceDecider implements RebalanceDecider {

    public static final String NAME = "deployment_in_progress";

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public Decision canRebalance(RebalanceContext context) {
        var observation = context.observation();
        if (observation.deployments().isEmpty() && observation.unreadable().stream()
            .noneMatch(key -> key.startsWith(org.opensearch.supervisor.model.Keys.DEPLOYMENTS_PREFIX))) {
            return Decision.yes(NAME, "no deployment is in progress");
        }
        DeploymentCoverage coverage = DeploymentCoverage.of(observation);
        if (coverage.poolFrozen()) {
            return Decision.throttle(NAME, "frozen", "a deployment in this pool cannot be parsed, so the pool is frozen"
                + " and nothing is moved for balance");
        }
        for (var covered : coverage.byNode().entrySet()) {
            if (covered.getValue().active() || covered.getValue().frozen()) {
                return Decision.throttle(NAME, "in_progress", "deployment [" + covered.getValue().id() + "] covers node ["
                    + covered.getKey() + "] in this pool, and a pool is not rebalanced while one is in progress");
            }
        }
        return Decision.yes(NAME, "no deployment in force covers a node of this pool");
    }
}
