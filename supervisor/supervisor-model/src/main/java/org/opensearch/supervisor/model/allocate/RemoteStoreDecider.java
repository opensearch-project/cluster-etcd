package org.opensearch.supervisor.model.allocate;

import org.opensearch.supervisor.model.NodeId;
import org.opensearch.supervisor.model.node.ShardRole;

import java.util.Optional;

/**
 * A search-only copy needs a node with a remote store to read its segments from.
 *
 * <p>Found by the real data plane, and worth stating carefully because the failure it prevents is <b>silent</b>.
 * OpenSearch chooses a replica's segment source from the <em>node's</em> attributes, and the branch it takes for a
 * node without a remote store resolves the primary through the local routing table — which a clusterless data
 * node's fabricated cluster state does not contain. So a search replica placed on such a node starts, reports
 * itself {@code STARTED}, and never receives a segment. It looks perfectly healthy and serves nothing.
 *
 * <p>The plugin already declines this, with a reason, so a placement here is not <em>unsafe</em> — it converges,
 * because the copy is reported absent and the diff keeps asking. What it wastes is a round trip per pass, for ever,
 * on a placement the supervisor had everything it needed to avoid: {@code registration} carries the node's
 * attributes. Cheaper to be right the first time, and much easier to read.
 *
 * <p><b>Keyed on the segment repository attribute, not on "is this a remote store node".</b> This mirrors the data
 * plane exactly, and the reason is specific: OpenSearch's {@code isRemoteStoreNode()} also requires a
 * cluster-state repository, which a node running {@code remote_store.mode=segments_only} does not have — and that
 * mode exists for this architecture. Gating on the broader predicate would refuse the very nodes this design
 * expects. The two sides must ask the same question or one will place what the other declines.
 *
 * <p>Only search replicas are checked. A primary's requirements are different and more involved — it may need a
 * translog repository to recover at all — and the plugin declines that case itself rather than starting a shard
 * empty over its own data. Encoding half of that rule here would be worse than leaving it there.
 */
public final class RemoteStoreDecider implements AllocationDecider {

    public static final String NAME = "remote_store";

    /**
     * The node attribute naming the repository segments are written to.
     *
     * <p>{@code RemoteStoreNodeAttribute.REMOTE_STORE_SEGMENT_REPOSITORY_NAME_ATTRIBUTE_KEY} in OpenSearch. Spelled
     * out rather than imported because this module deliberately depends on nothing.
     */
    public static final String SEGMENT_REPOSITORY_ATTRIBUTE = "remote_store.segment.repository";

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public Decision canAllocate(AllocationContext context, NodeId candidate) {
        if (context.role() != ShardRole.SEARCH_REPLICA) {
            return Decision.yes(NAME, "only a search-only copy has to read its segments from a remote store");
        }

        Optional<String> repository = repositoryOf(context, candidate);
        if (repository.isPresent()) {
            return Decision.yes(NAME, "node [" + candidate + "] writes segments to repository ["
                + repository.get() + "], which a search replica can read them from");
        }
        return Decision.no(NAME, "no_remote_store", "node [" + candidate + "] declares no ["
            + SEGMENT_REPOSITORY_ATTRIBUTE + "] attribute, so a search-only copy placed there would start,"
            + " report itself started, and never receive a segment — it has no store to read from and cannot"
            + " resolve a primary either");
    }

    /** The repository a node writes segments to, if it declares one. Blank counts as absent. */
    public static Optional<String> repositoryOf(AllocationContext context, NodeId candidate) {
        return context.attributeOf(candidate, SEGMENT_REPOSITORY_ATTRIBUTE)
            .filter(value -> value.isBlank() == false);
    }
}
