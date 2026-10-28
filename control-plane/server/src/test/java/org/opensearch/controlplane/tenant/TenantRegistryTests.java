/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.tenant;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.opensearch.controlplane.store.InMemoryKeyValueStore;
import org.opensearch.controlplane.store.KeyValueStore;
import org.opensearch.controlplane.store.RevisionConflictException;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The tenant registry: one root key per tenant, written so another service can enumerate them.
 *
 * <p>The interesting cases are all failure and repetition, because the happy path is a single put. What
 * matters is that it costs nothing after the first call, that losing a race is success, and that it can
 * never take down the request it is riding on.
 */
class TenantRegistryTests {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Clock FIXED = Clock.fixed(Instant.ofEpochMilli(1788969977933L), ZoneOffset.UTC);

    private static final TenantId ACME = TenantId.of("acme");

    @Test
    void registersATenantAtTheRoot() throws Exception {
        InMemoryKeyValueStore store = new InMemoryKeyValueStore();

        new TenantRegistry(store, FIXED).ensureRegistered(ACME);

        Optional<KeyValueStore.Versioned> stored = store.get("tenants/acme");
        assertTrue(stored.isPresent());
        JsonNode document = MAPPER.readTree(new String(stored.get().value(), StandardCharsets.UTF_8));
        assertEquals("acme", document.get("tenant").textValue());
        assertEquals(1788969977933L, document.get("registered_at").longValue());
    }

    /** From the injected clock, so this asserts a value rather than merely a field's presence. */
    @Test
    void theTimestampComesFromTheClock() throws Exception {
        InMemoryKeyValueStore store = new InMemoryKeyValueStore();
        Clock later = Clock.fixed(Instant.ofEpochMilli(42L), ZoneOffset.UTC);

        new TenantRegistry(store, later).ensureRegistered(ACME);

        JsonNode document = MAPPER.readTree(
            new String(store.get("tenants/acme").orElseThrow().value(), StandardCharsets.UTF_8));
        assertEquals(42L, document.get("registered_at").longValue());
    }

    /**
     * The per-process cache is the whole reason this is safe to call on every write. Without it each write
     * would pay a {@code get}, on the hot path, for something that changes once in a tenant's lifetime.
     */
    @Test
    void repeatedRegistrationTouchesTheStoreOnce() {
        CountingStore store = new CountingStore(new InMemoryKeyValueStore());
        TenantRegistry registry = new TenantRegistry(store, FIXED);

        for (int i = 0; i < 50; i++) {
            registry.ensureRegistered(ACME);
        }

        assertEquals(1, store.gets.get(), "one get, on the first call only");
        assertEquals(1, store.puts.get(), "one put, ever");
    }

    /** A tenant another process already registered is adopted without a second write. */
    @Test
    void anAlreadyRegisteredTenantIsNotRewritten() {
        InMemoryKeyValueStore delegate = new InMemoryKeyValueStore();
        delegate.put("tenants/acme", "{\"tenant\":\"acme\",\"registered_at\":1}".getBytes(StandardCharsets.UTF_8));
        CountingStore store = new CountingStore(delegate);

        new TenantRegistry(store, FIXED).ensureRegistered(ACME);

        assertEquals(0, store.puts.get(), "an existing registration is left alone");
    }

    /**
     * Two processes registering the same tenant at once: the loser gets a conflict, which is the correct
     * outcome rather than an error. Exercised through the exception path specifically, because that is the
     * branch a single-process test would otherwise never reach.
     */
    @Test
    void losingTheCreateRaceIsSuccess() {
        AtomicInteger conditionalPuts = new AtomicInteger();
        KeyValueStore contended = new ForwardingStore(new InMemoryKeyValueStore()) {
            @Override
            public long put(String key, byte[] value, long expectedRevision) {
                conditionalPuts.incrementAndGet();
                // Someone else created it between our get and our put.
                throw new RevisionConflictException(key, expectedRevision, 7L);
            }
        };
        TenantRegistry registry = new TenantRegistry(contended, FIXED);

        registry.ensureRegistered(ACME);
        registry.ensureRegistered(ACME);

        assertEquals(1, conditionalPuts.get(),
            "the conflict counts as registered, so it is not retried for the life of the process");
    }

    /**
     * A store failure must not reach the caller. The registry rides on somebody else's write, and failing
     * that write because a hint for another service could not be recorded would be strictly worse than the
     * supervisor's discovery lagging.
     */
    @Test
    void aStoreFailureIsSwallowed() {
        KeyValueStore broken = new ForwardingStore(new InMemoryKeyValueStore()) {
            @Override
            public long put(String key, byte[] value, long expectedRevision) {
                throw new IllegalStateException("backend unavailable");
            }
        };

        new TenantRegistry(broken, FIXED).ensureRegistered(ACME);
    }

    /** And it is retried, rather than being suppressed for the life of the process. */
    @Test
    void aTransientFailureIsRetriedOnTheNextWrite() {
        AtomicInteger attempts = new AtomicInteger();
        InMemoryKeyValueStore delegate = new InMemoryKeyValueStore();
        KeyValueStore flaky = new ForwardingStore(delegate) {
            @Override
            public long put(String key, byte[] value, long expectedRevision) {
                if (attempts.incrementAndGet() == 1) {
                    throw new IllegalStateException("backend unavailable");
                }
                return super.put(key, value, expectedRevision);
            }
        };
        TenantRegistry registry = new TenantRegistry(flaky, FIXED);

        registry.ensureRegistered(ACME);
        assertTrue(delegate.get("tenants/acme").isEmpty(), "the first attempt failed");

        registry.ensureRegistered(ACME);
        assertTrue(delegate.get("tenants/acme").isPresent(), "the second attempt succeeded");
    }

    @Test
    void theKeyIsOutsideEveryTenantPrefix() {
        assertEquals("tenants/acme", TenantRegistry.key(ACME));
        assertTrue(TenantRegistry.key(ACME).startsWith(ACME.keyPrefix()) == false);
        assertTrue(TenantRegistry.key(ACME).startsWith("tenant/") == false,
            "one character apart, and deliberately not overlapping");
    }

    /** Counts store calls without changing behaviour. */
    private static class CountingStore extends ForwardingStore {

        final AtomicInteger gets = new AtomicInteger();
        final AtomicInteger puts = new AtomicInteger();

        CountingStore(KeyValueStore delegate) {
            super(delegate);
        }

        @Override
        public Optional<Versioned> get(String key) {
            gets.incrementAndGet();
            return super.get(key);
        }

        @Override
        public long put(String key, byte[] value, long expectedRevision) {
            puts.incrementAndGet();
            return super.put(key, value, expectedRevision);
        }
    }

    /** Everything delegates, so a test can override exactly one method. */
    private abstract static class ForwardingStore implements KeyValueStore {

        private final KeyValueStore delegate;

        ForwardingStore(KeyValueStore delegate) {
            this.delegate = delegate;
        }

        @Override
        public Optional<Versioned> get(String key) {
            return delegate.get(key);
        }

        @Override
        public NavigableMap<String, Versioned> scanPrefix(String keyPrefix) {
            return delegate.scanPrefix(keyPrefix);
        }

        @Override
        public long put(String key, byte[] value) {
            return delegate.put(key, value);
        }

        @Override
        public long put(String key, byte[] value, long expectedRevision) {
            return delegate.put(key, value, expectedRevision);
        }

        @Override
        public boolean delete(String key) {
            return delegate.delete(key);
        }

        @Override
        public boolean delete(String key, long expectedRevision) {
            return delegate.delete(key, expectedRevision);
        }
    }
}
