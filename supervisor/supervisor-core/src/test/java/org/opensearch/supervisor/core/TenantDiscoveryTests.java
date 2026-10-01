package org.opensearch.supervisor.core;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.controlplane.store.InMemoryKeyValueStore;
import org.opensearch.supervisor.model.Keys;
import org.opensearch.supervisor.model.TenantId;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Runs against {@link InMemoryKeyValueStore}, taken from store-api's test fixtures rather than written
 * here. Two implementations of the same revision semantics is how the planes would silently disagree, and
 * that applies to a test double as much as to a backend.
 */
class TenantDiscoveryTests {

    private InMemoryKeyValueStore store;

    @BeforeEach
    void setUp() {
        store = new InMemoryKeyValueStore();
    }

    /** Writes what the control plane's {@code TenantRegistry} writes. */
    private void register(String tenant) {
        store.put(
            Keys.TENANTS_PREFIX + tenant,
            ("{\"tenant\":\"" + tenant + "\",\"registered_at\":1788969977933}").getBytes(StandardCharsets.UTF_8)
        );
    }

    @Test
    void readsTheRegistry() {
        register("acme");
        register("globex");

        var discovered = new TenantDiscovery(store, Set.of()).discover();

        assertEquals(Set.of(TenantId.of("acme"), TenantId.of("globex")), discovered.tenants());
        assertEquals(TenantDiscovery.Source.REGISTRY, discovered.source());
        assertEquals(2, discovered.fromRegistry());
    }

    @Test
    void fallsBackToConfigurationWhenTheRegistryIsEmpty() {
        // A fresh deployment and a control plane predating the registry look identical from here.
        // Reconciling nothing would be the wrong reading of both.
        var discovered = new TenantDiscovery(store, Set.of(TenantId.of("acme"))).discover();

        assertEquals(Set.of(TenantId.of("acme")), discovered.tenants());
        assertEquals(TenantDiscovery.Source.CONFIGURATION, discovered.source());
        assertEquals(0, discovered.fromRegistry());
    }

    @Test
    void configuredTenantsAreNeverOverruledByTheRegistry() {
        register("acme");

        var discovered = new TenantDiscovery(store, Set.of(TenantId.of("newco"))).discover();

        assertEquals(Set.of(TenantId.of("acme"), TenantId.of("newco")), discovered.tenants());
        assertEquals(TenantDiscovery.Source.REGISTRY, discovered.source());
    }

    @Test
    void oneUnreadableKeyDoesNotStopThePass() {
        // R2.5: torn or foreign data under our prefix is normal and must never be treated as corruption.
        // A tenant id the control plane could not have written must not prevent every other tenant from
        // being reconciled.
        register("acme");
        store.put(Keys.TENANTS_PREFIX + "not a tenant id", new byte[0]);

        var discovered = new TenantDiscovery(store, Set.of()).discover();

        assertEquals(Set.of(TenantId.of("acme")), discovered.tenants());
        assertEquals(List.of("not a tenant id"), discovered.malformed());
    }

    @Test
    void doesNotScanTheEntireStore() {
        // The whole reason the registry exists. A tenant with state must be discoverable without reading
        // its keys — so writing ten thousand index documents must not change what discovery returns or
        // what it had to read to get there.
        register("acme");
        for (int i = 0; i < 1000; i++) {
            store.put(Keys.indicesPrefix(TenantId.of("acme")) + "index-" + i, new byte[] { 1 });
        }

        var discovered = new TenantDiscovery(store, Set.of()).discover();

        assertEquals(Set.of(TenantId.of("acme")), discovered.tenants());
        assertEquals(1, discovered.fromRegistry());
        assertEquals(1, store.scanPrefix(Keys.TENANTS_PREFIX).size());
    }

    @Test
    void aTenantPrefixKeyIsNotATenantRegistration() {
        // "tenants/" and "tenant/" differ by one character and scanPrefix is specified over key prefixes
        // rather than path boundaries, so this is exactly the confusion the layout has to survive.
        store.put("tenant/acme/indices/my-index", new byte[] { 1 });

        var discovered = new TenantDiscovery(store, Set.of()).discover();

        assertTrue(discovered.tenants().isEmpty());
        assertEquals(TenantDiscovery.Source.CONFIGURATION, discovered.source());
    }
}
