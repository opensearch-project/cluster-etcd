package org.opensearch.supervisor.integ;

import org.junit.jupiter.api.condition.EnabledIf;

/**
 * The store assumptions the design rests on, checked against ZooKeeper.
 *
 * <p>All of the content is in {@link AbstractStoreAssumptionsIT}. This class supplies a store URI and nothing else, which is
 * the point: ZooKeeper and every other backend run the same assertion objects, so "does this rely on a
 * backend-specific guarantee?" cannot be answered differently by accident.
 */
@EnabledIf("org.opensearch.supervisor.integ.ZooKeeperTestServer#available")
class ZooKeeperStoreAssumptionsIT extends AbstractStoreAssumptionsIT {

    @Override
    String newStoreUri() {
        return ZooKeeperTestServer.storeUri(freshNamespace());
    }
}
