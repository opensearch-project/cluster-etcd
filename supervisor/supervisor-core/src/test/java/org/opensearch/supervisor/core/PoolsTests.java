package org.opensearch.supervisor.core;

import org.junit.jupiter.api.Test;
import org.opensearch.controlplane.store.InMemoryKeyValueStore;
import org.opensearch.supervisor.model.Keys;
import org.opensearch.supervisor.model.PoolId;
import org.opensearch.supervisor.model.pool.PoolDefinition;
import org.opensearch.supervisor.model.pool.PoolKind;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Reading the operator's pool documents, and refusing to guess when they contradict each other. */
class PoolsTests {

    private final InMemoryKeyValueStore store = new InMemoryKeyValueStore();

    private void define(String id, boolean isDefault) {
        PoolId pool = PoolId.of(id);
        store.put(Keys.poolDefinition(pool),
            new PoolDefinition(pool, PoolKind.SHARED, isDefault, null, null, List.of(), false).toBytes());
    }

    @Test
    void oneDefaultIsTheDefault() {
        define("shared-a", true);
        define("shared-b", false);

        Pools pools = Pools.read(store);

        assertEquals(PoolId.of("shared-a"), pools.defaultPool().orElseThrow());
        assertTrue(pools.problems().isEmpty(), pools.problems().toString());
    }

    @Test
    void twoDefaultsMeanNoDefault() {
        define("shared-a", true);
        define("shared-b", true);

        Pools pools = Pools.read(store);

        assertTrue(pools.defaultPool().isEmpty(), "chose " + pools.defaultPool());
        assertEquals(1, pools.problems().size(), pools.problems().toString());
    }

    @Test
    void aThirdDefaultDoesNotWinByScanOrder() {
        // Clearing the choice at each conflict as it was found left the third default standing, which is the scan
        // order deciding after all.
        define("shared-a", true);
        define("shared-b", true);
        define("shared-c", true);

        Pools pools = Pools.read(store);

        assertTrue(pools.defaultPool().isEmpty(), "chose " + pools.defaultPool());
        assertEquals(1, pools.problems().size(), "one problem, naming all of them: " + pools.problems());
        String problem = pools.problems().get(0);
        assertTrue(problem.contains("shared-a") && problem.contains("shared-b") && problem.contains("shared-c"),
            problem);
    }
}
