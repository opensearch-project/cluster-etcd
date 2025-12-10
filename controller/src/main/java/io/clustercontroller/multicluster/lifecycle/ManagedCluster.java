package io.clustercontroller.multicluster.lifecycle;

import io.clustercontroller.TaskManager;
import io.clustercontroller.multicluster.lock.ClusterLock;
import io.etcd.jetcd.Watch;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.concurrent.ScheduledFuture;

/**
 * Represents a cluster being actively managed with its associated resources.
 */
@Data
@AllArgsConstructor
public class ManagedCluster {
    private final String clusterId;
    private final TaskManager taskManager;
    private final ClusterLock lock;
    private final Watch.Watcher lockWatcher;
    private final ScheduledFuture<?> healthCheckTask;
}

