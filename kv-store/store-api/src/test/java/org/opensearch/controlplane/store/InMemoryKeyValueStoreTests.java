/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.store;

import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class InMemoryKeyValueStoreTests extends KeyValueStoreContractTests {

    @Override
    protected KeyValueStore newStore() {
        return new InMemoryKeyValueStore();
    }

    @Test
    void mutatingAReadArrayDoesNotAffectTheStore() {
        // Stronger than the contract requires. The interface only asks callers not to mutate; this
        // implementation copies on the way out too, so a buggy handler cannot corrupt a test double.
        InMemoryKeyValueStore store = new InMemoryKeyValueStore();
        store.put("a", bytes("one"));

        store.get("a").orElseThrow().value()[0] = 'Y';
        assertEquals(Optional.of("one"), store.getAsString("a"));
    }

    @Test
    void exposesSizeAndRevisionHelpersForTests() {
        InMemoryKeyValueStore store = new InMemoryKeyValueStore();
        assertEquals(0, store.size());
        assertEquals(KeyValueStore.ABSENT, store.revisionOf("a"));

        long revision = store.put("a", bytes("one"));
        assertEquals(1, store.size());
        assertEquals(revision, store.revisionOf("a"));
        assertTrue(store.toString().contains("a"));
    }
}
