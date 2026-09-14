/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package tools;

import org.opensearch.controlplane.store.KeyValueStore;
import org.opensearch.controlplane.store.KeyValueStores;

import java.nio.charset.StandardCharsets;

/**
 * Writes a pool definition into the store, because nothing else in the three planes can.
 *
 * <p>{@code pools/definitions/<pool-id>} is operator-owned by design: which nodes form a pool, which attributes
 * a shard's copies are spread across, and how many copies one node may hold all depend on the deployment rather
 * than on anything a service can observe. That is the right decision and it leaves a hole — <b>the whole system
 * does nothing at all until somebody writes one</b>, and no repository ships a way to do it. The symptom is
 * silence: the control plane accepts an index, the supervisor finds no pools and therefore runs no
 * reconciliation, the nodes register and heartbeat with nothing assigned, and the only thing anywhere that says
 * why is {@code unpooled_tenants} in {@code supervisor/status/housekeeping}.
 *
 * <p>Hand-writing the document is not an alternative, at least against the filesystem backend: it prefixes each
 * value with a revision header, so a file written with an editor is reported as {@code unreadable} rather than
 * as a pool.
 *
 * <p>This is a stopgap that belongs in the supervisor, which owns the key and already has a command line. Kept
 * here for now because the need showed up here.
 */
public final class SeedPool {

    private SeedPool() {}

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println("usage: SeedPool <store-uri> [pool-id] [awareness-attribute]");
            System.err.println();
            System.err.println("Writes pools/definitions/<pool-id> as the default pool, which is what makes an");
            System.err.println("unbound tenant reconcilable and a node declaring that pool placeable.");
            System.exit(2);
        }
        String uri = args[0];
        String pool = args.length > 1 ? args[1] : "shared-a";

        // No awareness attributes by default. Spreading copies across an attribute nobody chose would refuse
        // placements for a reason nobody asked for -- and on a laptop with one or two nodes there is nothing to
        // spread across anyway.
        String awareness = args.length > 2 ? "\"" + args[2] + "\"" : "";

        String definition = """
            {"pool":"%s","kind":"shared","default":true,\
            "description":"seeded by tools/SeedPool","max_shard_copies_per_node":2000,\
            "awareness_attributes":[%s]}""".formatted(pool, awareness);

        try (KeyValueStore store = KeyValueStores.open(uri)) {
            String key = "pools/definitions/" + pool;
            store.put(key, definition.getBytes(StandardCharsets.UTF_8));
            System.out.println("wrote " + key);
            System.out.println("  " + definition);
            System.out.println();
            System.out.println("A tenant with no pools/bindings/<tenant> entry is served by the default pool, so");
            System.out.println("nothing else needs writing. Nodes that declare this pool in clusterless.node.pool");
            System.out.println("become placeable on the supervisor's next pass.");
        }
    }
}
