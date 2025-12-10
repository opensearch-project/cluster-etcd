package io.clustercontroller.multicluster.registry;

import io.etcd.jetcd.support.CloseableClient;
import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * Represents a controller's registration with heartbeat.
 */
@Data
@AllArgsConstructor
public class ControllerRegistration {
    private final String controllerId;
    private final long leaseId;
    private final CloseableClient keepAliveObserver;
}

