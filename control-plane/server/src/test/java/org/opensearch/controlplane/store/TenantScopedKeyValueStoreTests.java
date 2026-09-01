/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.store;

import org.junit.jupiter.api.Test;
import org.opensearch.controlplane.tenant.TenantId;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TenantScopedKeyValueStoreTests {

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static String string(KeyValueStore.Versioned versioned) {
        return new String(versioned.value(), StandardCharsets.UTF_8);
    }

    private final InMemoryKeyValueStore root = new InMemoryKeyValueStore();
    private final KeyValueStore acme = new TenantScopedKeyValueStore(root, TenantId.of("acme"));
    private final KeyValueStore globex = new TenantScopedKeyValueStore(root, TenantId.of("globex"));

    @Test
    void writesLandUnderTheTenantPrefix() {
        acme.put("ingest/pipeline/p", bytes("A"));

        assertTrue(root.get("tenant/acme/ingest/pipeline/p").isPresent());
        assertTrue(root.get("ingest/pipeline/p").isEmpty(), "nothing should be written unprefixed");
    }

    @Test
    void tenantsCannotSeeEachOthersKeys() {
        acme.put("ingest/pipeline/p", bytes("acme value"));
        globex.put("ingest/pipeline/p", bytes("globex value"));

        assertEquals("acme value", string(acme.get("ingest/pipeline/p").orElseThrow()));
        assertEquals("globex value", string(globex.get("ingest/pipeline/p").orElseThrow()));
        assertEquals(2, root.size(), "the same logical key is two distinct rows");
    }

    @Test
    void scanPrefixStripsThePrefixSoCallersSeeTheirOwnKeySpace() {
        acme.put("ingest/pipeline/b", bytes("B"));
        acme.put("ingest/pipeline/a", bytes("A"));
        globex.put("ingest/pipeline/z", bytes("Z"));

        // Keys come back unprefixed, which is what lets PipelineStore.idFromKey stay
        // tenancy-unaware.
        assertEquals(
            List.of("ingest/pipeline/a", "ingest/pipeline/b"),
            List.copyOf(acme.scanPrefix("ingest/pipeline/").keySet())
        );
        assertEquals(List.of("ingest/pipeline/z"), List.copyOf(globex.scanPrefix("ingest/pipeline/").keySet()));
    }

    @Test
    void scanPrefixDoesNotLeakAcrossTenantsWithACommonNamePrefix() {
        // "acme" is a prefix of "acme-corp", so a naive implementation would leak.
        KeyValueStore acmeCorp = new TenantScopedKeyValueStore(root, TenantId.of("acme-corp"));
        acme.put("k", bytes("acme"));
        acmeCorp.put("k", bytes("acme-corp"));

        assertEquals(List.of("k"), List.copyOf(acme.scanPrefix("").keySet()));
        assertEquals("acme", string(acme.scanPrefix("").get("k")));
        assertEquals("acme-corp", string(acmeCorp.scanPrefix("").get("k")));
    }

    @Test
    void deleteOnlyAffectsTheOwningTenant() {
        acme.put("k", bytes("A"));
        globex.put("k", bytes("G"));

        assertTrue(acme.delete("k"));
        assertTrue(acme.get("k").isEmpty());
        assertTrue(globex.get("k").isPresent(), "the other tenant's key must survive");
        assertFalse(acme.delete("k"), "second delete finds nothing");
    }

    @Test
    void conditionalWritesArePerTenant() {
        acme.put("k", bytes("A"), KeyValueStore.ABSENT);
        // globex has no such key, so create-if-absent must still succeed for it.
        globex.put("k", bytes("G"), KeyValueStore.ABSENT);

        assertThrows(RevisionConflictException.class, () -> acme.put("k", bytes("A2"), KeyValueStore.ABSENT));
        assertEquals("G", string(globex.get("k").orElseThrow()));
    }

    @Test
    void revisionsPassThroughUnchanged() {
        long written = acme.put("k", bytes("A"));

        assertEquals(written, acme.get("k").orElseThrow().revision());
        assertEquals(written, root.revisionOf("tenant/acme/k"));
        // A revision obtained through the scoped view works for a conditional write through it.
        acme.put("k", bytes("A2"), written);
        assertEquals("A2", string(acme.get("k").orElseThrow()));
    }

    /**
     * A tenant ID containing {@code /} would let one tenant address another's key space, so it is
     * rejected at construction rather than sanitised later.
     */
    @Test
    void tenantIdRejectsPrefixEscapes() {
        assertThrows(IllegalArgumentException.class, () -> TenantId.of("acme/../globex"));
        assertThrows(IllegalArgumentException.class, () -> TenantId.of("acme/globex"));
        assertThrows(IllegalArgumentException.class, () -> TenantId.of(""));
        assertThrows(IllegalArgumentException.class, () -> TenantId.of("-leading-dash"));
        assertThrows(IllegalArgumentException.class, () -> TenantId.of("a".repeat(129)));

        assertEquals("tenant/acme_1.a-b/", TenantId.of("acme_1.a-b").keyPrefix());
    }

    @Test
    void emptyOptionalWhenTheTenantHasNothing() {
        assertEquals(Optional.empty(), acme.get("nothing"));
        assertTrue(acme.scanPrefix("").isEmpty());
    }
}
