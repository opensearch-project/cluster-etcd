/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.tenant;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.opensearch.controlplane.rest.Json;
import org.opensearch.controlplane.store.KeyValueStore;
import org.opensearch.controlplane.store.RevisionConflictException;

import java.time.Clock;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Records which tenants exist, so that another service can find them.
 *
 * <p>The control plane has no use for this. It is written for the supervisor, which reconciles every tenant
 * and therefore has to enumerate them — and the store cannot tell it: {@link KeyValueStore} has no
 * list-children or delimiter operation, so the only way to discover tenant ids from the data itself is
 * {@code scanPrefix("tenant/")}, which returns every key of every tenant. A full scan of the store to learn
 * a list of names.
 *
 * <p>The alternative on the supervisor's side is a configured list, which is wrong the first time a tenant
 * is onboarded without restarting it: the tenant is served here, has indices, and is silently unmanaged.
 * This exists to make that failure impossible to have quietly.
 *
 * <h2>One key, at the root</h2>
 *
 * <pre>{@code
 * tenants/acme     {"tenant":"acme","registered_at":1788969977933}
 * }</pre>
 *
 * <p>It has to be outside the tenant prefix — a marker at {@code tenant/<id>/registered} would need the very
 * scan this avoids to find. It is the <b>only</b> key the control plane writes outside a tenant prefix, and
 * {@code TenantIsolationTests} pins that down so it stays the only one.
 *
 * <p>Note {@code tenants/} and {@code tenant/} differ by one character and do not overlap as prefixes:
 * {@code "tenants/acme".startsWith("tenant/")} is false, because the character after {@code tenant} is
 * {@code s} rather than {@code /}. Close enough to be worth stating, since a scan written as
 * {@code scanPrefix("tenant")} without the slash would match both.
 *
 * <h2>Best effort, never fatal</h2>
 *
 * <p>A failure to register is logged and swallowed. This is a discovery hint for another plane, exactly like
 * the task records, and the correctness of neither service depends on it: a supervisor with a stale registry
 * reconciles fewer tenants than it should and can say so, whereas a control plane returning 500 because a
 * marker could not be written is strictly worse for everyone.
 *
 * <h2>Nothing removes it</h2>
 *
 * <p>A tenant whose keys are all gone may still be intended to exist, and the control plane cannot tell the
 * difference between a decommissioned tenant and an empty one. Removal is an operator action. The
 * supervisor's side of this is to report a registered tenant with no state rather than to guess.
 */
public final class TenantRegistry {

    /** The root prefix the supervisor scans. Outside every tenant prefix, deliberately. */
    public static final String KEY_PREFIX = "tenants/";

    static final String TENANT = "tenant";
    static final String REGISTERED_AT = "registered_at";

    private final KeyValueStore rootStore;
    private final Clock clock;

    /**
     * Tenants this process has already registered.
     *
     * <p>Not a cache of the store's contents — a bound on how often the store is asked. Without it every
     * write operation would pay an extra {@code get}, on the hot path, for information that changes once in
     * a tenant's lifetime.
     */
    private final Set<TenantId> known = ConcurrentHashMap.newKeySet();

    public TenantRegistry(KeyValueStore rootStore, Clock clock) {
        this.rootStore = Objects.requireNonNull(rootStore, "rootStore");
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    /** The registry key for a tenant. */
    public static String key(TenantId tenant) {
        return KEY_PREFIX + tenant.value();
    }

    /**
     * Records a tenant if it is not already recorded.
     *
     * <p>Safe to call on every write; after the first per tenant per process it is one set lookup.
     */
    public void ensureRegistered(TenantId tenant) {
        if (known.contains(tenant)) {
            return;
        }
        try {
            if (rootStore.get(key(tenant)).isEmpty()) {
                rootStore.put(key(tenant), Json.toBytes(document(tenant)), KeyValueStore.ABSENT);
            }
            known.add(tenant);
        } catch (RevisionConflictException lostTheRace) {
            // Another process registered it between the get and the put. The postcondition holds, so this is
            // success rather than an error -- and remembering it means this process stops asking.
            known.add(tenant);
        } catch (RuntimeException e) {
            // Deliberately not rethrown, and deliberately not remembered: a transient store failure should be
            // retried on the tenant's next write rather than suppressed for the life of the process.
            System.err.println("warning: could not register tenant [" + tenant
                + "]; supervisor discovery may lag until its next write. " + e);
        }
    }

    private ObjectNode document(TenantId tenant) {
        ObjectNode document = Json.object();
        document.put(TENANT, tenant.value());
        // From the injected clock, so a test can assert the value rather than only its presence. Costs
        // nothing and answers "when did this tenant first appear", which is otherwise unrecoverable.
        document.put(REGISTERED_AT, clock.millis());
        return document;
    }
}
