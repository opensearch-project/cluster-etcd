package org.opensearch.supervisor.core;

import org.opensearch.controlplane.store.KeyValueStore;
import org.opensearch.supervisor.model.Json;
import org.opensearch.supervisor.model.Keys;
import org.opensearch.supervisor.model.PoolId;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * How much work each pool is, read from the convergence document its owner publishes.
 *
 * <p>A member cannot measure another pool's size without observing it, and observing a pool it does not own
 * would be work done on someone else's behalf — so the owner publishes the figure and everyone else reads
 * it. That puts {@code supervisor/status/pools/<pool>/convergence} on the coordination path as well as the
 * operator path, which is worth knowing before changing its shape.
 *
 * <p>A pool nobody owns has no published weight, and gets the configured default. That is the right
 * direction to be wrong in: underestimating an unowned pool makes someone more likely to claim it, and an
 * unowned pool is worse than an unbalanced fleet.
 */
final class PoolWeights {

    private final KeyValueStore store;

    PoolWeights(KeyValueStore store) {
        this.store = Objects.requireNonNull(store, "store");
    }

    /**
     * Every pool, with a weight — <b>total, never partial.</b>
     *
     * <p>That is a contract rather than a convenience. An earlier version returned only the pools that had
     * published a weight, and the balancer summed that map to get the total while a member summed its own
     * holdings with a default for the unpublished ones. The two denominators disagreed, so with empty pools
     * — which is exactly what a fresh deployment has — total weight was zero, every member was permanently
     * over its fair share, and the fleet handed pools around for ever. Keeping the map total removes the
     * possibility rather than fixing the symptom.
     */
    Map<PoolId, Double> read(Set<PoolId> pools, double defaultWeight) {
        Map<PoolId, Double> weights = new LinkedHashMap<>();
        for (PoolId pool : pools) {
            weights.put(pool, defaultWeight);
            store.get(Keys.poolConvergence(pool)).ifPresent(versioned -> {
                try {
                    var node = Json.mapper().readTree(versioned.value()).get("weight");
                    if (node != null && node.isNumber() && node.asDouble() > 0) {
                        weights.put(pool, node.asDouble());
                    }
                } catch (Exception e) {
                    // An unreadable convergence document costs a default weight and nothing else. It must
                    // never stop the balancer, which would leave pools unowned over a cosmetic key.
                }
            });
        }
        return weights;
    }
}
