package org.opensearch.supervisor.nodecontract;

import org.opensearch.supervisor.model.NodeId;

import java.time.Duration;
import java.util.List;

/**
 * A data plane node, reduced to what {@link NodeContractTests} needs to drive it.
 *
 * <p>{@code NODE_CONTRACT.md} has two independent implementations — {@code fake-node} and the real plugin
 * in {@code ../opensearch-data-plane} — and two implementations of one document format that are only
 * tested separately are two formats. This is the adapter that lets one suite of assertions run against
 * both, in the manner of {@code KeyValueStoreContractTests} in the store repository.
 *
 * <p>Deliberately narrow. Everything here is something both implementations can honestly do; anything one
 * can do and the other cannot belongs in that implementation's own tests, not behind a capability flag
 * here. A conformance suite that half its implementations skip is a suite that proves nothing about
 * either.
 */
public interface ContractNode {

    /** The persistent id, which is what every key this node writes is suffixed with. */
    NodeId nodeId();

    /** Fresh per process, and echoed on all three documents so a restart is visible without a re-read. */
    String ephemeralId();

    /** Announce this process: write {@code nodes/registration/<id>}. */
    void start();

    /**
     * One pass: reconcile, then write whichever documents changed, then heartbeat.
     *
     * <p>Explicit rather than a background thread, for the same reason the supervisor's pass is: a pass
     * that only advances when asked is a pass a test can reason about.
     */
    void pass();

    /**
     * Move this node's clock forward.
     *
     * <p>Needed because two of the contract's properties are about time passing without anything else
     * changing: the heartbeat must be rewritten anyway, and the inventory must not be.
     */
    void advanceClock(Duration by);

    /**
     * Bring this node to holding exactly these copies, serving, taking as many passes as it needs.
     *
     * <p>How is the implementation's business — {@code fake-node} writes itself an assignment and ticks
     * until it has converged, and a real node is handed an inventory to report — because the suite is
     * about the documents that come out, not about how a node decided what to hold.
     *
     * <p>Only serving copies, and that is a limitation worth naming: a copy that is still initializing is
     * a state one implementation reaches by ticking once and the other by being told, so assertions about
     * it live in each implementation's own tests. What is shared is everything about a converged node,
     * which is where a format drifts.
     */
    void holdServing(List<ContractCopy> copies);

    /**
     * What this node made of an assignment document.
     *
     * <p><b>The other direction, which the rest of this interface is silent about.</b> Every other method here is
     * about documents a node <em>writes</em>, and every check reads them back out of the store. The assignment is
     * the one document flowing the other way — the supervisor writes it, the node reads it — so nothing shared
     * asserted that both implementations read it the same way, because there was no shared way to ask a node what
     * it understood.
     *
     * <p>That gap cost a running deployment. A {@code roles} array replaced a {@code role} string, the plugin's
     * reader knew only the old spelling, and its coordinator quietly deferred every tenant with "role is not one
     * this node understands". Neither service's several hundred tests noticed, and neither did the suite whose
     * entire purpose is stopping the two drifting apart on a format.
     *
     * <p>{@code fake-node} cannot catch it either, structurally: it writes its own assignment in order to
     * converge, so it reads a document produced by the same code that produced its expectations, and a format
     * change moves both together. Which is why the checks that use this feed it documents built by <b>the
     * supervisor's own writer</b>. Hand-written JSON in either repository only tests that repository's opinion of
     * the format.
     *
     * @param document the bytes of a {@code nodes/assignment/<id>} document
     */
    Understood readAssignment(byte[] document);

    /**
     * A node's reading of an assignment, in terms the suite can compare.
     *
     * <p>Deliberately not the parsed {@code Assignment} object: that would test that both sides call the same
     * parser, which is the thing not in question. What is in question is what each side <em>concluded</em>, so
     * this is stated as the two things a node acts on — which tenants it does what for, and which copies it is
     * meant to hold.
     *
     * @param roles per tenant, and never empty for a tenant that appears. A role the node did not recognise must
     *     show as {@link org.opensearch.supervisor.model.node.NodeRole#UNKNOWN} rather than being dropped, so
     *     that "understood by nothing" is distinguishable from "no roles" — silently empty is the failure this
     *     whole seam exists to catch.
     * @param copies every shard copy the document asks for, carrying the index name and config revision, because
     *     those are what a node builds against and a format change can lose them without losing anything else.
     * @param primaryTerms the term the node will fabricate for each of those copies, 1 where the document gave none.
     *     Every copy must appear. It is the value the node <em>acts on</em>, not the parse: a node that read the field
     *     and then fabricated 1 regardless is the failure {@code contract_features} exists to rule out, and only
     *     reporting what it will use can catch it.
     */
    record Understood(
        java.util.Map<org.opensearch.supervisor.model.TenantId,
            java.util.Set<org.opensearch.supervisor.model.node.NodeRole>> roles,
        List<ContractCopy> copies,
        java.util.Map<ContractCopy, Long> primaryTerms
    ) {

        public Understood {
            roles = java.util.Map.copyOf(roles);
            copies = List.copyOf(copies);
            primaryTerms = java.util.Map.copyOf(primaryTerms);
        }

        /**
         * Without terms, for an implementation that has not been extended yet. It fails the term checks rather than
         * passing them, deliberately: an implementation that says nothing about terms is one nobody has checked.
         */
        public Understood(
            java.util.Map<org.opensearch.supervisor.model.TenantId,
                java.util.Set<org.opensearch.supervisor.model.node.NodeRole>> roles,
            List<ContractCopy> copies
        ) {
            this(roles, copies, java.util.Map.of());
        }

        /** Copies sorted, so two implementations that agree are not reported as differing on iteration order. */
        public List<ContractCopy> sortedCopies() {
            return copies.stream()
                .sorted(java.util.Comparator.comparing(ContractCopy::toString))
                .toList();
        }
    }
}
