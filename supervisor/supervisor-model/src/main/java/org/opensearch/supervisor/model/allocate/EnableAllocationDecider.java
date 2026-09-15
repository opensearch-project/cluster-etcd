package org.opensearch.supervisor.model.allocate;

import org.opensearch.supervisor.model.NodeId;
import org.opensearch.supervisor.model.node.ShardRole;
import org.opensearch.supervisor.model.settings.AllocationEnable;

/**
 * Honours {@code routing.allocation.enable} — the operator's brake.
 *
 * <p>"Stop moving my data around while I restart these machines" is a thing an operator legitimately needs to
 * say, and a reconciler that cannot be told to stop is one nobody will trust with a production cluster. This is
 * the only decider whose answer comes from an instruction rather than from the state of the cluster, which is
 * why its refusal reads differently: nothing is wrong, somebody asked.
 *
 * <p>The index setting beats the cluster one, so a single index can be frozen without freezing the rest.
 *
 * <p>Two values are treated conservatively, and both say so in their reasons rather than deciding quietly. An
 * <b>unrecognised</b> value is a value an operator meant something by, so refusing is safer than assuming
 * {@code all} and silently ignoring an instruction to stop. And {@code new_primaries} is applied as
 * {@code none}, because distinguishing a never-allocated shard from one whose copies have all gone needs
 * per-shard history that nothing here keeps — see {@link AllocationEnable#NEW_PRIMARIES}.
 */
public final class EnableAllocationDecider implements AllocationDecider {

    public static final String NAME = "enable_allocation";

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public Decision canAllocate(AllocationContext context, NodeId candidate) {
        AllocationEnable effective = context.allocationEnable();
        boolean primary = context.role() == ShardRole.PRIMARY;

        if (effective.allows(primary)) {
            return Decision.yes(NAME, "allocation is enabled for this " + (primary ? "primary" : "replica")
                + " (routing.allocation.enable=" + effective.name().toLowerCase(java.util.Locale.ROOT) + ")");
        }

        return switch (effective) {
            case NONE -> Decision.no(NAME, "none", "allocation is disabled by routing.allocation.enable=none;"
                + " nothing is wrong with this cluster, somebody asked it to stop");
            case PRIMARIES -> Decision.no(NAME, "primaries_only", "routing.allocation.enable=primaries allows primaries only,"
                + " so this replica waits until the setting is relaxed");
            case NEW_PRIMARIES -> Decision.no(NAME, "new_primaries", "routing.allocation.enable=new_primaries is applied as"
                + " none, because telling a never-allocated shard from one whose copies have all gone needs"
                + " per-shard history this supervisor does not keep; refusing is the safe direction, since an"
                + " operator who set this was protecting the cluster from data movement");
            default -> Decision.no(NAME, "unrecognised", "routing.allocation.enable is set to a value this build does not"
                + " recognise, so nothing is allocated; guessing [all] would silently ignore an instruction"
                + " to stop");
        };
    }
}
