package org.opensearch.supervisor.integ;

import org.junit.jupiter.api.condition.EnabledIf;

/**
 * End-to-end convergence, checked against ZooKeeper.
 *
 * <p>All of the content is in {@link AbstractConvergenceIT}. This class supplies a store URI and nothing else, which is
 * the point: ZooKeeper and every other backend run the same assertion objects, so "does this rely on a
 * backend-specific guarantee?" cannot be answered differently by accident.
 */
@EnabledIf("org.opensearch.supervisor.integ.ZooKeeperTestServer#available")
class ZooKeeperConvergenceIT extends AbstractConvergenceIT {

    @Override
    String newStoreUri() {
        return ZooKeeperTestServer.storeUri(freshNamespace());
    }
}
