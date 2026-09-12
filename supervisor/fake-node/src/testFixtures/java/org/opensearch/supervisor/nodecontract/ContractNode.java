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
}
