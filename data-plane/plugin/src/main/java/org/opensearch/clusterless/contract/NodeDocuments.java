/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.clusterless.contract;

import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;

/**
 * Turns the three documents this plugin writes into the bytes {@code NODE_CONTRACT.md} specifies, and
 * applies each one's size budget.
 *
 * <p><b>XContent rather than Jackson databind,</b> which is a deliberate difference from the supervisor
 * and fake-node. Those two share {@code supervisor-model}'s codec; this plugin re-implements it. The
 * reason is packaging: the server exposes {@code jackson-core} but not {@code jackson-databind}, so
 * sharing the codec would mean shipping databind inside the plugin zip alongside a server that has its
 * own Jackson on the classpath. XContent is already here, produces the same JSON, and adds nothing to the
 * distribution. The cost — two implementations of one format — is paid for by the shared conformance
 * suite in the supervisor repository, which parses these exact bytes with the supervisor's own reader.
 *
 * <p>Three properties of the output are load-bearing rather than incidental:
 *
 * <ul>
 *   <li><b>Deterministic.</b> Fields are written in a fixed order and maps in sorted key order, because
 *       the heartbeat announces a digest of the inventory's exact bytes. Re-serialising an unchanged
 *       inventory into differently ordered JSON would announce a new digest and make the supervisor
 *       re-read something that had not changed.
 *   <li><b>Null-omitting.</b> A field with nothing to say is absent, never {@code null} and never a zero
 *       standing in for one. {@code size_bytes} is the case that matters: zero is a real size.
 *   <li><b>Budgeted.</b> A document over its budget is not written at all. A truncated inventory is worse
 *       than a missing one, because the supervisor reads absent copies as <em>gone</em> and starts
 *       rebuilding shards that exist (D3.6).
 * </ul>
 */
public final class NodeDocuments {

    /** Nothing in the heartbeat scales, so this can only be exceeded by a bug. */
    public static final int HEARTBEAT_BUDGET_BYTES = 4 * 1024;

    public static final int REGISTRATION_BUDGET_BYTES = 4 * 1024;

    /** Well clear of ZooKeeper's 1 MiB default and etcd's 1.5 MiB. About 5,600 copies per node. */
    public static final int INVENTORY_BUDGET_BYTES = 512 * 1024;

    private NodeDocuments() {}

    // ---------------------------------------------------------------- registration

    public static byte[] write(Registration registration) {
        return budgeted(build(builder -> {
            builder.field("node_id", registration.nodeId().value());
            builder.field("ephemeral_id", registration.ephemeralId());
            builder.field("name", registration.name());
            if (registration.pool() != null) {
                builder.field("pool", registration.pool().value());
            }
            List<String> roles = new ArrayList<>();
            for (NodeRole role : registration.roles()) {
                roles.add(role.wireName());
            }
            builder.field("roles", roles);
            builder.field("address", registration.address());
            builder.field("port", registration.port());
            builder.field("version", registration.version());
            builder.field("attributes");
            builder.map(registration.attributes());
            builder.field("heartbeat_interval_millis", registration.heartbeatIntervalMillis());
            builder.field("registered_at", registration.registeredAt());
        }), "registration", REGISTRATION_BUDGET_BYTES);
    }

    // ---------------------------------------------------------------- heartbeat

    public static byte[] write(Heartbeat heartbeat) {
        return budgeted(build(builder -> {
            builder.field("node_id", heartbeat.nodeId().value());
            builder.field("ephemeral_id", heartbeat.ephemeralId());
            builder.field("timestamp", heartbeat.timestamp());

            Heartbeat.Applied applied = heartbeat.applied();
            builder.startObject("applied");
            builder.field("assignment_version", applied.assignmentVersion());
            builder.field("outcome", applied.outcome().wireName());
            if (applied.detail() != null) {
                builder.field("detail", applied.detail());
            }
            builder.endObject();

            if (heartbeat.shardsDigest() != null) {
                builder.field("shards_digest", heartbeat.shardsDigest());
            }
            builder.field("shard_copies", heartbeat.shardCopies());

            // Absent when nothing has happened, which is the convention the rest of these documents follow and
            // the right one here: a field that appears only when it means something is much easier to alert on
            // than one that is usually zero.
            Heartbeat.RegistrationRewrites rewrites = heartbeat.registrationRewrites();
            if (rewrites != null) {
                builder.startObject("registration_rewrites");
                builder.field("count", rewrites.count());
                builder.field("last_at", rewrites.lastAt());
                builder.endObject();
            }

            Heartbeat.Capacity capacity = heartbeat.capacity();
            builder.startObject("capacity");
            builder.field("disk_total_bytes", capacity.diskTotalBytes());
            builder.field("disk_available_bytes", capacity.diskAvailableBytes());
            builder.field("heap_used_percent", capacity.heapUsedPercent());
            builder.field("cpu_used_percent", capacity.cpuUsedPercent());
            builder.field("load_1m", capacity.load1m());
            builder.endObject();
        }), "heartbeat", HEARTBEAT_BUDGET_BYTES);
    }

    // ---------------------------------------------------------------- inventory

    /**
     * The flat copy list, nested by tenant and index on the way out.
     *
     * <p>{@link ShardInventory} keeps its copies sorted by {@code (tenant, index uuid, shard)}, so this
     * walk emits tenants and indices in sorted order and closes each object exactly when the key changes.
     * No intermediate maps, and the ordering the digest depends on comes from the record rather than from
     * whatever a map's iterator happened to do.
     */
    public static byte[] write(ShardInventory inventory) {
        return budgeted(build(builder -> {
            builder.field("node_id", inventory.nodeId().value());
            builder.field("ephemeral_id", inventory.ephemeralId());
            builder.field("generated_at", inventory.generatedAt());
            builder.startObject("tenants");

            TenantId openTenant = null;
            String openIndex = null;
            for (ShardInventory.HeldCopy copy : inventory.copies()) {
                if (copy.tenant().equals(openTenant) == false) {
                    if (openIndex != null) {
                        builder.endObject(); // shards
                        builder.endObject(); // the index
                        openIndex = null;
                    }
                    if (openTenant != null) {
                        builder.endObject(); // indices
                        builder.endObject(); // the tenant
                    }
                    openTenant = copy.tenant();
                    builder.startObject(openTenant.value());
                    builder.startObject("indices");
                }
                if (copy.indexUuid().equals(openIndex) == false) {
                    if (openIndex != null) {
                        builder.endObject(); // shards
                        builder.endObject(); // the index
                    }
                    openIndex = copy.indexUuid();
                    builder.startObject(openIndex);
                    builder.field("name", copy.indexName());
                    builder.field("config_revision", copy.configRevision());
                    builder.startObject("shards");
                }
                builder.startObject(Integer.toString(copy.shard()));
                builder.field("role", copy.role().wireName());
                builder.field("state", copy.state().wireName());
                if (copy.sizeBytes() != null) {
                    builder.field("size_bytes", copy.sizeBytes());
                }
                if (copy.reason() != null) {
                    builder.field("reason", copy.reason());
                }
                builder.endObject();
            }
            if (openIndex != null) {
                builder.endObject(); // shards
                builder.endObject(); // the index
            }
            if (openTenant != null) {
                builder.endObject(); // indices
                builder.endObject(); // the tenant
            }

            builder.endObject(); // tenants
        }), "shard inventory", INVENTORY_BUDGET_BYTES);
    }

    // ---------------------------------------------------------------- the digest handshake

    /**
     * Hex SHA-256 of the <b>exact bytes</b> of a stored document, prefixed with the algorithm.
     *
     * <p>Over bytes rather than a canonical form on purpose: the supervisor digests what it read and
     * compares it with what this node claimed, so neither side has to agree on a canonical serialisation —
     * only on which bytes were stored. That is also what makes it correct across the two independent
     * codecs this seam now has.
     */
    public static String digest(byte[] documentBytes) {
        try {
            MessageDigest sha256 = MessageDigest.getInstance("SHA-256");
            return "sha256:" + HexFormat.of().formatHex(sha256.digest(documentBytes));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is required of every JVM", e);
        }
    }

    // ---------------------------------------------------------------- plumbing

    /** What a builder callback may do. Declared so the lambdas above can throw {@link IOException}. */
    @FunctionalInterface
    private interface Body {
        void write(XContentBuilder builder) throws IOException;
    }

    private static byte[] build(Body body) {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (XContentBuilder builder = XContentType.JSON.contentBuilder(bytes)) {
            builder.startObject();
            body.write(builder);
            builder.endObject();
        } catch (IOException e) {
            // Writing to a byte array cannot fail for any reason a caller could act on.
            throw new UncheckedIOException(e);
        }
        return bytes.toByteArray();
    }

    private static byte[] budgeted(byte[] bytes, String what, int budget) {
        if (bytes.length > budget) {
            throw new DocumentTooLargeException(what, bytes.length, budget);
        }
        return bytes;
    }
}
