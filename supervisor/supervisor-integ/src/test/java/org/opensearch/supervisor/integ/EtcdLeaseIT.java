package org.opensearch.supervisor.integ;

import org.junit.jupiter.api.condition.EnabledIf;

/**
 * Several supervisors contending for pools, checked against etcd.
 *
 * <p>All of the content is in {@link AbstractLeaseIT}. This class supplies a store URI and nothing else, which is
 * the point: etcd and every other backend run the same assertion objects, so "does this rely on a
 * backend-specific guarantee?" cannot be answered differently by accident.
 */
@EnabledIf("org.opensearch.supervisor.integ.EtcdTestServer#available")
class EtcdLeaseIT extends AbstractLeaseIT {

    @Override
    String newStoreUri() {
        return EtcdTestServer.storeUri(freshNamespace());
    }
}
