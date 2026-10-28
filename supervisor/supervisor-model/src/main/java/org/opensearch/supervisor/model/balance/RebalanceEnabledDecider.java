package org.opensearch.supervisor.model.balance;

import org.opensearch.supervisor.model.allocate.Decision;
import org.opensearch.supervisor.model.settings.AllocationEnable;
import org.opensearch.supervisor.model.settings.RebalanceEnable;

/**
 * The operator's tidiness brake, and the one they will actually reach for.
 *
 * <p>Two settings, checked in the order of how much they mean. {@code cluster.routing.allocation.enable} comes
 * first: a move is an allocation followed by a removal, so an operator who has stopped allocation has stopped
 * moves too, whatever the rebalance setting says. Then
 * {@code cluster.routing.rebalance.enable}, which stops moves without stopping recovery.
 *
 * <p>Both refusals are worded to say which setting did it, because the failure this prevents is an operator
 * disabling one knob, seeing movement continue, and concluding the supervisor is ignoring them.
 */
public final class RebalanceEnabledDecider implements RebalanceDecider {

    public static final String NAME = "enable_rebalance";

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public Decision canRebalance(RebalanceContext context) {
        AllocationEnable allocation = context.allocationEnable();
        if (allocation.allows(context.isPrimary()) == false) {
            return Decision.no(NAME, "allocation_disabled", "allocation is set to [" + allocation.name().toLowerCase(java.util.Locale.ROOT)
                + "], and a move is an allocation followed by a removal, so nothing can move until allocation is"
                + " allowed again — the rebalance setting is not the one holding this up");
        }

        RebalanceEnable rebalance = context.rebalanceEnable();
        if (rebalance.allows(context.isPrimary())) {
            return Decision.yes(NAME, "rebalancing is set to ["
                + rebalance.name().toLowerCase(java.util.Locale.ROOT) + "], which allows moving a "
                + (context.isPrimary() ? "primary" : "replica"));
        }

        if (context.rebalanceEnableIsRecognised() == false) {
            return Decision.no(NAME, "unrecognised", "cluster.routing.rebalance.enable is set to something this does not"
                + " recognise, so it is being read as [none]; an unrecognised value is treated as a request to"
                + " stop rather than guessed at, and the cost of being wrong here is only an untidy pool");
        }
        return Decision.no(NAME, "role_not_allowed", "rebalancing is set to [" + rebalance.name().toLowerCase(java.util.Locale.ROOT)
            + "], which does not allow moving a " + (context.isPrimary() ? "primary" : "replica"));
    }
}
