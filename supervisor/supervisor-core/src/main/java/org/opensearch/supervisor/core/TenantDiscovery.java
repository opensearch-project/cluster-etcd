package org.opensearch.supervisor.core;

import org.opensearch.controlplane.store.KeyValueStore;
import org.opensearch.supervisor.model.Keys;
import org.opensearch.supervisor.model.TenantId;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

/**
 * Answers "which tenants exist?".
 *
 * <p>The store cannot list children. {@code KeyValueStore} has {@code get} and {@code scanPrefix} and no
 * delimiter operation, so {@code scanPrefix("tenant/")} returns not the tenant ids but every key of every
 * tenant — the entire store, on every pass, to learn a list of names. The control plane therefore writes a
 * root registry key per tenant on that tenant's first write, which makes discovery one small scan.
 *
 * <p>Two lists are involved and conflating them is how a tenant goes silently unmanaged:
 *
 * <ul>
 *   <li>tenants that <b>exist</b> — this class, from {@code tenants/}
 *   <li>tenants that are <b>managed</b> — {@code pools/bindings/}, written by an operator
 * </ul>
 *
 * <p>A tenant in the first and not the second is entirely unmanaged: no shards placed, no health computed,
 * no tasks reaped, all of which look exactly like an idle tenant. Detecting that needs both lists, which is
 * the whole reason the registry was worth a change to another service.
 *
 * <p>Deliberately not level-triggering-relevant: this is an input to observation, not a source of
 * intent. It never decides that a tenant has been <em>deleted</em>, only that one has appeared.
 */
public final class TenantDiscovery {

    /** Where a discovered tenant came from, reported so that a fallback cannot be mistaken for the real thing. */
    public enum Source {
        /** The control plane's registry answered. */
        REGISTRY,
        /** The registry was empty or absent, so the configured list was used. */
        CONFIGURATION
    }

    /**
     * @param tenants the union of both sources, sorted for stable iteration
     * @param source where the bulk of the answer came from
     * @param fromRegistry how many the registry supplied
     * @param fromConfiguration how many the configured list supplied
     * @param malformed registry keys that were not valid tenant ids, and were skipped rather than thrown on
     */
    public record Discovered(
        Set<TenantId> tenants,
        Source source,
        int fromRegistry,
        int fromConfiguration,
        List<String> malformed
    ) {
        public Discovered {
            tenants = Set.copyOf(tenants);
            malformed = List.copyOf(malformed);
        }
    }

    private final KeyValueStore store;
    private final Set<TenantId> configured;

    /**
     * @param configured the fallback list, used when the registry is empty. Always unioned in, so that an
     *     operator naming a tenant explicitly is never overruled by a registry that has not caught up.
     */
    public TenantDiscovery(KeyValueStore store, Collection<TenantId> configured) {
        this.store = Objects.requireNonNull(store, "store");
        this.configured = Set.copyOf(Objects.requireNonNull(configured, "configured"));
    }

    public Discovered discover() {
        Set<TenantId> tenants = new TreeSet<>(configured);
        List<String> malformed = new java.util.ArrayList<>();
        int fromRegistry = 0;

        for (String key : store.scanPrefix(Keys.TENANTS_PREFIX).keySet()) {
            String id = Keys.idAfter(Keys.TENANTS_PREFIX, key);
            try {
                tenants.add(TenantId.of(id));
                fromRegistry++;
            } catch (IllegalArgumentException e) {
                // Torn or foreign data under our prefix must not stop the pass. R2.5: this is normal, not
                // corruption. Report it and carry on — one unreadable key cannot be allowed to prevent
                // every other tenant from being reconciled.
                malformed.add(id);
            }
        }

        // The registry being empty is not the same as it being wrong: a fresh deployment, or a control
        // plane that predates the registry, both look like this. Falling back rather than reconciling
        // nothing is the safe reading, and saying which happened is why Source exists.
        Source source = fromRegistry > 0 ? Source.REGISTRY : Source.CONFIGURATION;

        return new Discovered(tenants, source, fromRegistry, configured.size(), malformed);
    }
}
