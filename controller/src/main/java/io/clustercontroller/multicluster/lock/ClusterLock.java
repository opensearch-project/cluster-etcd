package io.clustercontroller.multicluster.lock;

import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.support.CloseableClient;
import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * Represents an acquired lock on a cluster with associated lease and keep-alive.
 */
@Data
@AllArgsConstructor
public class ClusterLock {
    private final String clusterId;
    private final long leaseId;
    private final ByteSequence lockKey;
    private final CloseableClient keepAliveObserver;
}
