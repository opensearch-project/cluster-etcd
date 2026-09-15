/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.clusterless.contract;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/**
 * {@code tenant/<t>/status/indices/<index-uuid>} — where every copy of an index's shards is, as the supervisor
 * last observed it. Written by the supervisor, read here.
 *
 * <p>The contract calls this the answer to a question the assignment deliberately does not answer. An assignment
 * tells a node what <em>it</em> should hold; a coordinator has to know where every copy of its tenants' shards
 * lives, and so, under document replication, does a data node. Putting that table in each node's assignment would
 * store it once per node and rewrite all of them whenever one shard moved, so it lives here instead: one document
 * per index, four readers — {@code cluster.health}, {@code cluster.state}, coordinator routing and docrep peer
 * resolution.
 *
 * <p><b>It lists copies that are not yet started, and that is load-bearing.</b> The contract is explicit: a
 * projection filtered to {@code STARTED} would deadlock a replica placed in the same pass as its primary, because
 * the replica could never see the primary it is waiting for. So a reader gets every observed copy with its state
 * and decides for itself what to do with an {@code INITIALIZING} one.
 *
 * <p>Read tolerantly, like every other document in this package: unknown fields are ignored, unknown enum values
 * become {@code UNKNOWN} rather than failing the parse, and a document this build cannot use at all costs one pass
 * rather than the node. A supervisor newer than this node is the ordinary case during an upgrade.
 */
public record IndexStatus(
    String name,
    String uuid,
    long configRevision,
    int numberOfShards,
    int numberOfReplicas,
    Replication replication,
    Map<Integer, List<Copy>> shards
) {

    public IndexStatus {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(uuid, "uuid");
        replication = replication == null ? Replication.UNKNOWN : replication;
        shards = shards == null ? Map.of() : Collections.unmodifiableMap(new TreeMap<>(shards));
    }

    /**
     * One copy of one shard, on one node.
     *
     * @param serving the supervisor's own judgement, kept because it folds together "started" and "on a node that
     *     is still alive" — two facts a reader would otherwise have to join for itself
     */
    public record Copy(NodeId node, ShardRole role, ShardState state, boolean serving) {

        public Copy {
            Objects.requireNonNull(node, "node");
            role = role == null ? ShardRole.UNKNOWN : role;
            state = state == null ? ShardState.UNKNOWN : state;
        }
    }

    /**
     * How an index replicates, which decides whether its copies need peers at all.
     *
     * <p>The contract's point: this is a property of the index rather than of the assignment, which is why a bare
     * role string is enough for a segment-replication copy — it pulls from the remote store and resolves nobody.
     */
    public enum Replication {
        SEGMENT,
        DOCUMENT,
        UNKNOWN;

        public static Replication from(String value) {
            if (value == null) {
                return UNKNOWN;
            }
            return switch (value.trim().toUpperCase(Locale.ROOT)) {
                case "SEGMENT" -> SEGMENT;
                case "DOCUMENT" -> DOCUMENT;
                default -> UNKNOWN;
            };
        }
    }

    /** Every copy of every shard, flattened — for a reader that does not care which shard is which. */
    public List<Copy> allCopies() {
        List<Copy> all = new ArrayList<>();
        shards.values().forEach(all::addAll);
        return all;
    }

    public List<Copy> copiesOf(int shard) {
        return shards.getOrDefault(shard, List.of());
    }

    /** Whether this describes the index the reader meant. Names are reused; uuids are not. */
    public boolean matches(String indexUuid) {
        return uuid.equals(indexUuid);
    }

    public static IndexStatus read(byte[] bytes) {
        String what = "index status";
        Map<String, Object> document = Documents.parse(bytes, what);

        String name = Documents.requireString(document, "index", what);
        String uuid = Documents.requireString(document, "index_uuid", what);

        Map<Integer, List<Copy>> shards = new TreeMap<>();
        Documents.object(document, "shards").forEach((shardNumber, value) -> {
            int shard;
            try {
                shard = Integer.parseInt(shardNumber);
            } catch (NumberFormatException e) {
                throw new MalformedDocumentException(what + ": shard [" + shardNumber + "] is not a number", e);
            }
            shards.put(shard, readCopies(value, what));
        });

        return new IndexStatus(
            name,
            uuid,
            Documents.number(document, "config_revision", 0, what),
            (int) Documents.number(document, "number_of_shards", 0, what),
            (int) Documents.number(document, "number_of_replicas", 0, what),
            Replication.from(Documents.string(document, "replication", null)),
            shards
        );
    }

    private static List<Copy> readCopies(Object value, String what) {
        if (value instanceof List<?> == false) {
            return List.of();
        }
        List<Copy> copies = new ArrayList<>();
        for (Object element : (List<?>) value) {
            if (element instanceof Map<?, ?> == false) {
                continue;
            }
            @SuppressWarnings("unchecked")
            Map<String, Object> copy = (Map<String, Object>) element;
            copies.add(
                new Copy(
                    NodeId.of(Documents.requireString(copy, "node_id", what)),
                    ShardRole.from(Documents.string(copy, "role", null)),
                    ShardState.from(Documents.string(copy, "state", null)),
                    Boolean.parseBoolean(Documents.string(copy, "serving", "false"))
                )
            );
        }
        return List.copyOf(copies);
    }
}
