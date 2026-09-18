package org.opensearch.supervisor.model.plan;

import org.opensearch.supervisor.model.NodeId;
import org.opensearch.supervisor.model.allocate.AllocationContext;
import org.opensearch.supervisor.model.allocate.Allocator;
import org.opensearch.supervisor.model.node.ShardRole;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Gives up a search-only copy so that a primary can be placed where it is the only thing in the way.
 *
 * <p>The deadlock this breaks was found by running a real cluster down to one node: the survivor held a shard's
 * <b>search replica</b>, the node that held its <b>primary</b> was long dead, and {@code SameShardDecider}
 * correctly refused the only candidate for the primary because it already held a copy. Every rule behaved as
 * designed and the index could never be written to again — not after a wait, not after a supervisor restart.
 *
 * <p><b>A primary and a read-only copy competing for the last node is not a tie.</b> Reads survive losing a
 * copy; writes do not survive losing their primary, and a search replica cannot become one — upstream has no
 * promotion path for a search-only copy, so the copy's presence is pure obstruction rather than a head start.
 *
 * <h2>Drop first, place afterwards</h2>
 *
 * This plans only the removal. The primary is placed on a later pass by the ordinary path, once the node reports
 * the copy gone, and no state is kept between the two — the next pass re-derives the whole thing from what it
 * observes, like everything else here.
 *
 * <p>The one-write alternative is real and is deliberately not taken yet: an assignment holds one role per shard,
 * so changing {@code {0: search_replica}} to {@code {0: primary}} is a single field in a single document with no
 * window at all. It needs the data plane to turn a read-only copy into a writeable one in place, including
 * removing an index-level {@code index.blocks.search_only}, so it is a coordinated change across two
 * repositories. Dropping and placing needs nothing from anybody and makes the index writable again; the cost is
 * that the copy is rebuilt from the remote store rather than reused, and a window where the shard serves no
 * reads either. Both are better than a shard that can never be written to.
 */
public final class Displacer {

    /** A serving copy to give up, and the primary it is being given up for. */
    public record Displacement(NodeId node, ShardRole role, String because) {}

    /**
     * @param displacement the copy to withdraw, or null
     * @param because when nothing is being displaced, a sentence to add to the refusal the operator will read —
     *     or null when there was nothing to consider at all. Worth saying: once a shard has a read-only copy in
     *     the way, {@code same_shard}'s "already holds a copy" reads as <em>"delete that and this would work"</em>,
     *     and it is important to know when that is false.
     */
    public record Outcome(Displacement displacement, String because) {

        static final Outcome NOTHING_TO_CONSIDER = new Outcome(null, null);

        static Outcome declined(String because) {
            return new Outcome(null, because);
        }

        public boolean displacing() {
            return displacement != null;
        }
    }

    private Displacer() {}

    /**
     * Whether giving up a read-only copy would let this primary be placed.
     *
     * <p>Only ever called after a real placement attempt has failed, and it answers the question by <em>asking
     * the allocator again</em> against a context where those copies are treated as gone — not by reading the
     * refusal and inferring the rest. That distinction is the safety property: a node refused for disk as well as
     * for holding the copy would otherwise lose a serving copy and gain nothing.
     *
     * @param context the context the failed attempt used, whose role must be {@code PRIMARY}
     */
    public static Outcome propose(AllocationContext context, Allocator allocator, List<NodeId> candidates) {
        if (context.role() != ShardRole.PRIMARY) {
            return Outcome.NOTHING_TO_CONSIDER;
        }

        Set<NodeId> readOnlyHolders = new LinkedHashSet<>();
        Set<NodeId> alreadyWithdrawn = new LinkedHashSet<>();
        for (NodeId candidate : candidates) {
            if (context.roleOfCopyOn(candidate).orElse(null) != ShardRole.SEARCH_REPLICA) {
                continue;
            }
            readOnlyHolders.add(candidate);
            if (context.observation().nodes().get(candidate)
                .isAssigned(context.tenant(), context.index().uuid(), context.shard()) == false) {
                // Already told to drop it and still reporting it. Re-planning the removal every pass would be an
                // instruction that writes nothing, spends a slot against the work ceiling and claims in the plan
                // to have dropped a copy -- so the wait gets said out loud instead.
                alreadyWithdrawn.add(candidate);
            }
        }

        if (readOnlyHolders.isEmpty()) {
            return Outcome.NOTHING_TO_CONSIDER;
        }
        if (alreadyWithdrawn.size() == readOnlyHolders.size()) {
            return Outcome.declined("the search-only copy in the way has already been withdrawn from "
                + names(alreadyWithdrawn) + "; waiting for the copy to be dropped before placing the primary");
        }

        Allocator.Outcome hypothetical = allocator.choose(
            context.withoutCopiesOfThisShardOn(readOnlyHolders), candidates
        );
        if (hypothetical.placed() == false) {
            return Outcome.declined("giving up the search-only copy on " + names(readOnlyHolders)
                + " would not help, so it is kept: " + hypothetical.refusal().because());
        }

        NodeId target = hypothetical.placement().node();
        if (readOnlyHolders.contains(target) == false) {
            // The primary would land somewhere that holds no copy of this shard, which means the obstruction was
            // indirect -- an awareness or address rule counting the read-only copy against a different node. That
            // may well be worth acting on, but "delete a serving copy on A so that C can take a primary" is a
            // longer argument than this rule has made, so it is reported rather than done.
            return Outcome.declined("without the search-only copy on " + names(readOnlyHolders)
                + " the primary would be placed on [" + target.value() + "], which holds no copy of this shard;"
                + " that is a rule this build does not act on");
        }
        if (alreadyWithdrawn.contains(target)) {
            return Outcome.declined("the search-only copy on [" + target.value() + "] has already been withdrawn;"
                + " waiting for it to be dropped before placing the primary");
        }

        return new Outcome(
            new Displacement(target, ShardRole.SEARCH_REPLICA,
                "giving up the search-only copy on this node so its shard can have a primary again: a search"
                    + " replica cannot be promoted, reads survive losing a copy and writes do not survive losing"
                    + " a primary, and this node is the only one that can take it"),
            null
        );
    }

    private static String names(Set<NodeId> nodes) {
        return nodes.stream().map(node -> "[" + node.value() + "]").reduce((a, b) -> a + ", " + b).orElse("[]");
    }
}
