package org.opensearch.supervisor.model.allocate;

import org.opensearch.supervisor.model.NodeId;

/**
 * Two copies of the same shard never share a node.
 *
 * <p>The rule without which redundancy is decoration: a replica on the same machine as its primary survives
 * nothing that the primary does not. It is also the rule that makes a naive "emptiest node wins" allocator
 * wrong — that allocator would put every copy of every shard on whichever node started out with the most disk.
 *
 * <h2>Sharing an address is a separate, optional rule</h2>
 *
 * <p>Two nodes on one host are one failure domain whatever their ids say, so this also refuses a candidate whose
 * address matches that of a node already holding a copy — <b>but only when the pool asks for it</b>
 * ({@code refuse_copies_sharing_a_host}). Off by default, for three reasons:
 *
 * <ul>
 *   <li><b>OpenSearch does the same.</b> {@code cluster.routing.allocation.same_shard.host} defaults to
 *       {@code false} — "no check is performed by default" — and this supervisor should not be quietly stricter
 *       than the server it is placing shards for.
 *   <li><b>An address is wrong in both directions.</b> Too strict for several nodes deliberately run on one
 *       host, and far too weak in Kubernetes, where two pods on one physical machine have different addresses
 *       and share a failure domain completely. Anything treating an address as a failure domain is wrong on most
 *       modern deployments, in the dangerous direction.
 *   <li><b>There is a mechanism that is actually right.</b> {@code awarenessAttributes} spreads copies across
 *       whatever an operator says the failure domains are, and only an operator knows. This rule is a guess
 *       standing in for that, so it defaults to not guessing.
 * </ul>
 *
 * <p>It was on unconditionally, and the cost was concrete rather than theoretical: every node of a
 * single-machine deployment shares {@code 127.0.0.1}, so no replica of anything could ever be placed. That is
 * every developer's first two-node test, and it presented as a shard that stayed unassigned while the diff
 * insisted two nodes could take it.
 */
public final class SameShardDecider implements AllocationDecider {

    public static final String NAME = "same_shard";

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public Decision canAllocate(AllocationContext context, NodeId candidate) {
        if (context.nodesWithThisShard().contains(candidate)) {
            return Decision.no(NAME, "already_holds", "node [" + candidate + "] already holds, is assigned, or is about to be"
                + " given a copy of this shard; a second copy on one node adds no redundancy at all");
        }

        if (context.refuseCopiesSharingAHost() == false) {
            return Decision.yes(NAME, "node [" + candidate + "] holds no copy of this shard");
        }

        String candidateAddress = addressOf(context, candidate);
        if (candidateAddress == null) {
            return Decision.yes(NAME, "node [" + candidate + "] holds no copy of this shard");
        }

        for (NodeId other : context.nodesWithThisShard()) {
            String otherAddress = addressOf(context, other);
            if (candidateAddress.equals(otherAddress)) {
                return Decision.no(NAME, "shared_host", "node [" + candidate + "] shares address " + candidateAddress
                    + " with node [" + other + "], which already has a copy, and this pool refuses copies that"
                    + " share a host");
            }
        }
        return Decision.yes(NAME, "node [" + candidate + "] shares neither this shard nor a host with a copy of it");
    }

    private static String addressOf(AllocationContext context, NodeId nodeId) {
        var node = context.observation().nodes().get(nodeId);
        return node == null || node.registration() == null ? null : node.registration().address();
    }
}
