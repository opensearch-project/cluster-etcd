/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.clusterless.publish;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.clusterless.contract.AppliedOutcome;
import org.opensearch.clusterless.contract.DocumentTooLargeException;
import org.opensearch.clusterless.contract.Heartbeat;
import org.opensearch.clusterless.contract.Keys;
import org.opensearch.clusterless.contract.NodeDocuments;
import org.opensearch.clusterless.contract.NodeId;
import org.opensearch.clusterless.contract.Registration;
import org.opensearch.clusterless.contract.ShardInventory;
import org.opensearch.controlplane.store.KeyValueStore;

import java.util.List;
import java.util.Objects;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/**
 * Publishes the three documents this node owns: {@code nodes/registration/<id>},
 * {@code nodes/shards/<id>} and {@code nodes/heartbeat/<id>}.
 *
 * <p>One pass is {@link #publish()}, and it is explicit rather than a background loop for the same reason
 * the supervisor's pass is: a pass that only advances when asked is a pass a test can reason about. The
 * scheduling lives in {@code ScheduledPasses}, which does nothing but call this on a timer.
 *
 * <h2>The write order is the contract's, and it is not incidental</h2>
 *
 * <p>Registration, then inventory, then heartbeat. The middle one is the load-bearing pair: the heartbeat
 * announces a digest of the inventory's bytes, so writing it first would have the supervisor chase an
 * inventory that does not exist yet. In this order a mismatch always means "the heartbeat is behind the
 * inventory", which is safe to act on. The same ordering is what lets the supervisor prefer the inventory
 * over the heartbeat when the two straddle a restart.
 *
 * <h2>What is written when</h2>
 *
 * <ul>
 *   <li><b>Registration</b> on start, and afterwards only if it would say something different. The
 *       comparison deliberately excludes {@code registered_at}, or it would differ every pass.
 *   <li><b>Inventory</b> only when the copies change. Not "when the bytes change" — when the
 *       <em>copies</em> change: a node that recomputed and re-stamped {@code generated_at} every pass
 *       would produce a new digest every pass, and the document that grows with shard count would be back
 *       on the hot path, which is the one thing the three-document split exists to prevent (D3.4).
 *   <li><b>Heartbeat</b> every pass, unconditionally. Its {@code timestamp} guarantees the bytes change,
 *       and the supervisor's liveness check is "did this key's revision advance" — so a heartbeat
 *       suppressed as unchanged would make a healthy node look dead.
 * </ul>
 *
 * <h2>Failure</h2>
 *
 * <p>A store that cannot be reached leaves this node <b>serving what it already has</b> (D6.5). Losing the
 * store is losing the ability to change, not the ability to work; a node that stopped answering queries
 * because it could not reach a coordination service would make the store a single point of failure for the
 * data path. So every failure here is logged and recorded, and the next pass tries again.
 *
 * <p>State is updated only after a write has actually landed, which makes the retry correct rather than
 * merely harmless: an inventory whose write failed is not remembered as written, so the next pass writes
 * it, and the digest the heartbeat announces never describes bytes that are not in the store.
 */
public final class NodePublisher {

    private static final Logger logger = LogManager.getLogger(NodePublisher.class);

    private final KeyValueStore store;
    private final NodeId nodeId;
    private final String ephemeralId;
    private final Supplier<Registration> registrationSource;
    private final ShardInventorySource inventorySource;
    private final CapacityProbe capacityProbe;
    private final LongSupplier clock;

    /** The last document of each kind this node actually got into the store. */
    private Registration writtenRegistration;
    private ShardInventory writtenInventory;
    private String writtenInventoryDigest;
    private long writtenInventoryAt;
    private long writtenHeartbeatAt;

    /** What the assignment applier last managed. Nothing sets it yet; the assignment reader will. */
    private Heartbeat.Applied applied = Heartbeat.Applied.NONE;

    private String lastFailure;
    private int consecutiveFailures;

    public NodePublisher(
        KeyValueStore store,
        NodeId nodeId,
        String ephemeralId,
        Supplier<Registration> registrationSource,
        ShardInventorySource inventorySource,
        CapacityProbe capacityProbe,
        LongSupplier clock
    ) {
        this.store = Objects.requireNonNull(store, "store");
        this.nodeId = Objects.requireNonNull(nodeId, "nodeId");
        this.ephemeralId = Objects.requireNonNull(ephemeralId, "ephemeralId");
        this.registrationSource = Objects.requireNonNull(registrationSource, "registrationSource");
        this.inventorySource = Objects.requireNonNull(inventorySource, "inventorySource");
        this.capacityProbe = Objects.requireNonNull(capacityProbe, "capacityProbe");
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    /**
     * How far this node has got with its assignment, echoed on the next heartbeat.
     *
     * <p>Called from whichever thread applies an assignment, which is not the thread that publishes, hence
     * the lock. A hint and not a source of truth: a supervisor that ignored it entirely would still
     * converge from the inventory.
     */
    public synchronized void reportApplied(Heartbeat.Applied applied) {
        this.applied = Objects.requireNonNull(applied, "applied");
    }

    /**
     * One pass.
     *
     * <p>Never throws. A pass that propagated an exception would kill the scheduled task on some
     * executors and silently stop this node heartbeating, which the supervisor would read as the node
     * being gone and answer by rebuilding its shards somewhere else — a store hiccup turned into a
     * recovery storm.
     *
     * <p>The three writes are guarded separately, and <b>the heartbeat is always attempted</b>. That is
     * the difference that matters: the other two failing produces states the contract already has answers
     * for — a heartbeat with no registration is {@code NODE_UNREGISTERED}, and a heartbeat whose digest
     * does not match the inventory means the inventory write has not landed — while the heartbeat failing
     * produces the one state that gets shards rebuilt. Skipping it because something earlier went wrong
     * would turn a recoverable problem into an eviction.
     */
    public synchronized void publish() {
        long now = clock.getAsLong();
        String problem = null;

        try {
            publishRegistration(now);
        } catch (Exception e) {
            problem = "registration: " + describe(e);
            logFailure("registration", e);
        }

        InventoryOutcome inventory;
        try {
            inventory = publishInventory(now);
        } catch (Exception e) {
            // The last inventory stands. Publishing an empty one instead would say every copy is gone,
            // and the supervisor would believe it — the most destructive reading available of "I could
            // not work out what I hold".
            inventory = new InventoryOutcome(
                writtenInventory == null ? 0 : writtenInventory.copyCount(),
                "shard inventory: " + describe(e)
            );
            logFailure("shard inventory", e);
        }
        if (problem == null) {
            problem = inventory.detail();
        }

        try {
            publishHeartbeat(now, inventory);
        } catch (Exception e) {
            problem = "heartbeat: " + describe(e);
            logFailure("heartbeat", e);
        }

        if (problem == null) {
            if (consecutiveFailures > 0) {
                logger.info("clusterless: publishing recovered after {} failed passes", consecutiveFailures);
            }
            consecutiveFailures = 0;
            lastFailure = null;
        } else {
            consecutiveFailures++;
            lastFailure = problem;
        }
    }

    /**
     * Loud once, then quiet.
     *
     * <p>An unreachable store produces one of these per interval, and a log filling at that rate buries
     * whatever else the node is trying to say about the outage — including the message saying it
     * recovered.
     */
    private void logFailure(String what, Exception e) {
        if (consecutiveFailures == 0) {
            logger.warn("clusterless: could not publish the {}; this node keeps serving what it has", what, e);
        } else {
            logger.debug("clusterless: could not publish the {} ({} consecutive failed passes)", what, consecutiveFailures, e);
        }
    }

    private void publishRegistration(long now) {
        Registration current = registrationSource.get();
        if (current.sameAs(writtenRegistration)) {
            return;
        }
        // Keep the moment this process first registered rather than the moment an attribute changed. The
        // field says when this node registered, and a node that had been up for a day should not look as
        // though it had just arrived because somebody relabelled its rack.
        long registeredAt = writtenRegistration == null ? now : writtenRegistration.registeredAt();
        Registration stamped = new Registration(
            current.nodeId(),
            current.ephemeralId(),
            current.name(),
            current.pool(),
            current.roles(),
            current.address(),
            current.port(),
            current.version(),
            current.attributes(),
            current.heartbeatIntervalMillis(),
            registeredAt
        );
        store.put(Keys.nodeRegistration(nodeId), NodeDocuments.write(stamped));
        writtenRegistration = stamped;
        logger.info(
            "clusterless: registered node [{}] ephemeral [{}] pool [{}] roles {} heartbeat every {}ms",
            nodeId,
            ephemeralId,
            stamped.pool() == null ? "(none declared)" : stamped.pool(),
            stamped.roles(),
            stamped.heartbeatIntervalMillis()
        );
    }

    /** What the heartbeat needs to know about how the inventory went. */
    private record InventoryOutcome(int copyCount, String detail) {}

    private InventoryOutcome publishInventory(long now) {
        List<ShardInventory.HeldCopy> copies = inventorySource.heldCopies();
        ShardInventory candidate = new ShardInventory(nodeId, ephemeralId, now, copies);

        if (writtenInventory != null && writtenInventory.sameCopiesAs(candidate)) {
            // Unchanged: the previously written document stands, generated_at and all. This is the branch
            // that keeps the inventory off the hot path, and taking it on almost every pass is the point.
            return new InventoryOutcome(writtenInventory.copyCount(), null);
        }

        byte[] bytes;
        try {
            bytes = NodeDocuments.write(candidate);
        } catch (DocumentTooLargeException e) {
            // D3.6: write nothing, and say so. A truncated inventory is worse than a stale one — the
            // supervisor reads copies that are absent from it as gone and starts rebuilding shards that
            // exist. The heartbeat keeps announcing the digest of the last document actually written,
            // which is what that field means, while shard_copies reports the real count. The disagreement
            // between the two is the signal that something is wrong here.
            logger.error("clusterless: not writing the shard inventory: {}", e.getMessage());
            return new InventoryOutcome(candidate.copyCount(), e.getMessage());
        }

        store.put(Keys.nodeShards(nodeId), bytes);
        writtenInventory = candidate;
        writtenInventoryDigest = NodeDocuments.digest(bytes);
        writtenInventoryAt = now;
        logger.debug("clusterless: wrote inventory with {} copies, digest {}", candidate.copyCount(), writtenInventoryDigest);
        return new InventoryOutcome(candidate.copyCount(), null);
    }

    private void publishHeartbeat(long now, InventoryOutcome inventory) {
        Heartbeat.Applied toReport = inventory.detail() == null
            ? applied
            // An inventory this node could not publish outranks whatever the applier had to say: the
            // supervisor's whole model of this node comes from a document that is now stale, and that is
            // the more urgent fact. The assignment version is kept so the ack still says what was applied.
            : Heartbeat.Applied.failed(applied.assignmentVersion(), inventory.detail());

        Heartbeat heartbeat = new Heartbeat(
            nodeId,
            ephemeralId,
            now,
            toReport,
            writtenInventoryDigest,
            inventory.copyCount(),
            capacityProbe.capacity()
        );
        store.put(Keys.nodeHeartbeat(nodeId), NodeDocuments.write(heartbeat));
        writtenHeartbeatAt = now;
    }

    /**
     * What this node last wrote, for the local diagnostic D7.3 asks for.
     *
     * <p>"Why is <em>this</em> node behaving like that" is a different question from "what does the fleet
     * look like", and it is the one asked at 3am. The supervisor's projections answer the first; nothing
     * but the node can answer the second.
     */
    public synchronized PublishStatus status() {
        return new PublishStatus(
            nodeId,
            ephemeralId,
            writtenRegistration != null,
            writtenRegistration == null ? 0 : writtenRegistration.registeredAt(),
            writtenInventoryDigest,
            writtenInventory == null ? -1 : writtenInventory.copyCount(),
            writtenInventory == null ? 0 : writtenInventory.generatedAt(),
            writtenInventoryAt,
            writtenHeartbeatAt,
            applied.assignmentVersion(),
            applied.outcome(),
            consecutiveFailures,
            lastFailure
        );
    }

    /**
     * A snapshot of what this node last published.
     *
     * @param inventoryGeneratedAt when the copies last changed, which is <em>not</em> when the inventory
     *     was last written — the two differ by design, and seeing both is how an operator confirms the
     *     document is being left alone on purpose rather than failing to be written
     */
    public record PublishStatus(
        NodeId nodeId,
        String ephemeralId,
        boolean registered,
        long registeredAt,
        String inventoryDigest,
        int inventoryCopies,
        long inventoryGeneratedAt,
        long inventoryWrittenAt,
        long heartbeatWrittenAt,
        long appliedAssignmentVersion,
        AppliedOutcome appliedOutcome,
        int consecutiveFailures,
        String lastFailure
    ) {}

    private static String describe(Exception e) {
        String message = e.getMessage();
        return e.getClass().getSimpleName() + (message == null ? "" : ": " + message);
    }
}
