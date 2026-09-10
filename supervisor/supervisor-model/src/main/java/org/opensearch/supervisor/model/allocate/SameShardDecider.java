package org.opensearch.supervisor.model.allocate;

import org.opensearch.supervisor.model.NodeId;

/**
 * Two copies of the same shard never share a node.
 *
 * <p>The rule without which redundancy is decoration: a replica on the same machine as its primary survives
 * nothing that the primary does not. It is also the rule that makes a naive "emptiest node wins" allocator
 * wrong — that allocator would put every copy of every shard on whichever node started out with the most disk.
 *
 * <p>Also refuses a node sharing an <em>address</em> with one that already holds a copy. Two nodes on one host
 * are one failure domain whatever their ids say, and a deployment that runs several nodes per machine is
 * common enough that discovering this the hard way is a real risk.
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
            return Decision.no(NAME, "node [" + candidate + "] already holds, is assigned, or is about to be"
                + " given a copy of this shard; a second copy on one node adds no redundancy at all");
        }

        String candidateAddress = addressOf(context, candidate);
        if (candidateAddress == null) {
            return Decision.yes(NAME, "node [" + candidate + "] holds no copy of this shard");
        }

        for (NodeId other : context.nodesWithThisShard()) {
            String otherAddress = addressOf(context, other);
            if (candidateAddress.equals(otherAddress)) {
                return Decision.no(NAME, "node [" + candidate + "] shares address " + candidateAddress
                    + " with node [" + other + "], which already has a copy; two nodes on one host are one"
                    + " failure domain whatever their ids say");
            }
        }
        return Decision.yes(NAME, "node [" + candidate + "] shares neither this shard nor a host with a copy of it");
    }

    private static String addressOf(AllocationContext context, NodeId nodeId) {
        var node = context.observation().nodes().get(nodeId);
        return node == null || node.registration() == null ? null : node.registration().address();
    }
}
