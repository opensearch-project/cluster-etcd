package org.opensearch.supervisor.model.node;

import org.opensearch.supervisor.model.Json;

/**
 * Reads and writes the four documents of {@code NODE_CONTRACT.md}, applying each one's size budget.
 *
 * <p>The encoding itself lives in {@link Json}, which every document in this system shares. What is here is
 * the part specific to the node seam: which budget applies to which document, and the digest handshake.
 */
public final class NodeJson {

    /** Nothing in the heartbeat scales, so this can only be exceeded by a bug. */
    public static final int HEARTBEAT_BUDGET_BYTES = 4 * 1024;

    public static final int REGISTRATION_BUDGET_BYTES = 4 * 1024;

    /** Well clear of ZooKeeper's 1 MiB default and etcd's 1.5 MiB. About 5,600 copies per node. */
    public static final int INVENTORY_BUDGET_BYTES = 512 * 1024;

    public static final int ASSIGNMENT_BUDGET_BYTES = 512 * 1024;

    private NodeJson() {}

    // ---------------------------------------------------------------- writing

    public static byte[] write(Registration registration) {
        return writeChecked(registration, "registration", REGISTRATION_BUDGET_BYTES);
    }

    public static byte[] write(Heartbeat heartbeat) {
        return writeChecked(heartbeat, "heartbeat", HEARTBEAT_BUDGET_BYTES);
    }

    public static byte[] write(ShardInventory inventory) {
        return writeChecked(inventory, "shard inventory", INVENTORY_BUDGET_BYTES);
    }

    public static byte[] write(Assignment assignment) {
        return writeChecked(assignment, "assignment", ASSIGNMENT_BUDGET_BYTES);
    }

    private static byte[] writeChecked(Object document, String what, int budget) {
        byte[] bytes = Json.write(document, what);
        if (bytes.length > budget) {
            throw new DocumentTooLargeException(what, bytes.length, budget);
        }
        return bytes;
    }

    // ---------------------------------------------------------------- reading

    public static Registration readRegistration(byte[] bytes) {
        return Json.read(bytes, Registration.class, "registration");
    }

    public static Heartbeat readHeartbeat(byte[] bytes) {
        return Json.read(bytes, Heartbeat.class, "heartbeat");
    }

    public static ShardInventory readInventory(byte[] bytes) {
        return Json.read(bytes, ShardInventory.class, "shard inventory");
    }

    public static Assignment readAssignment(byte[] bytes) {
        return Json.read(bytes, Assignment.class, "assignment");
    }

    // ---------------------------------------------------------------- the inventory digest

    /**
     * The digest the heartbeat announces: hex SHA-256 of the exact bytes written to
     * {@code nodes/shards/<node-id>}. See {@link Json#digest}.
     */
    public static String digest(byte[] documentBytes) {
        return Json.digest(documentBytes);
    }

    /** For diagnostics and tests. */
    public static String toJson(Object document) {
        return Json.toJson(document);
    }
}
